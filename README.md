# TarnMedia

A small, readable WebRTC SFU written in Go on top of [Pion](https://github.com/pion/webrtc).
It forwards audio, camera and screen-share tracks between participants of a room,
authorizes them with JWTs issued by *your* application, and does nothing else.

TarnMedia is the media backend of [TarnVeil](https://tarnveil.ru), a small chat and
voice application, and is extracted from it. It is roughly 1,500 lines of Go, so
reading the whole thing is a realistic way to understand how an SFU works.

## Status

**Experimental.** It runs in production for one small deployment (rooms of up to
25 participants) and has unit tests, but:

- there is **no simulcast or SVC layer selection** — every subscriber receives the
  publisher's single encoding, so a large room is as expensive as its worst link;
- there are **no sustained load tests**, and no published capacity numbers;
- the API is not frozen and may change between commits;
- there is one maintainer and no support commitment. See [SECURITY.md](SECURITY.md)
  for what that means for vulnerability reports.

Use it to learn, to prototype, or as a starting point you are willing to maintain
yourself. If you need a hardened, well-staffed SFU today, use LiveKit or mediasoup.

## What it does

- authenticated, room-scoped WebSocket signaling at `GET /v1/ws`;
- one Opus audio track plus camera and screen video inputs per participant;
- forwarding of browser-negotiated standard codecs (Opus, VP8, H.264 and anything
  both endpoints support) without transcoding;
- trickle ICE, renegotiation, periodic video keyframe requests, exact `Origin`
  allowlisting;
- a bounded UDP port range and optional public-IP configuration;
- a loopback-only authenticated control API for evicting users and closing rooms;
- token revocation, signaling rate limits, WebSocket keepalives, and `/health`,
  `/ready` and Prometheus `/metrics` endpoints.

It deliberately does **not** do recording, transcoding, SIP, simulcast selection,
or persistence.

## Quick start

```bash
export TARNMEDIA_JWT_SECRET='a-random-secret-at-least-32-characters-long'
export TARNMEDIA_CONTROL_SECRET='another-random-secret-at-least-32-chars'
go run ./cmd/tarnmedia
```

On Windows PowerShell:

```powershell
$env:TARNMEDIA_JWT_SECRET = 'a-random-secret-at-least-32-characters-long'
$env:TARNMEDIA_CONTROL_SECRET = 'another-random-secret-at-least-32-chars'
go run ./cmd/tarnmedia
```

The public HTTP/WebSocket handler listens on `:8088`, media uses UDP
`50000-50100`, and the control API plus `/metrics` listen on `127.0.0.1:8089`.
By default only `http://localhost:5173` and `http://127.0.0.1:5173` are accepted
as browser origins — every real deployment must set its own.

TarnMedia also reads `.env` and `../.env` from the working directory; environment
variables take precedence. See [`.env.example`](.env.example).

### Docker

```bash
docker build -t tarnmedia .
docker run --network host \
  -e TARNMEDIA_JWT_SECRET=... \
  -e TARNMEDIA_CONTROL_SECRET=... \
  -e TARNMEDIA_ALLOWED_ORIGINS=https://app.example \
  tarnmedia
```

`--network host` is not a shortcut here. WebRTC needs the container to see and
advertise the address peers will actually reach; with bridge networking you must
publish the whole UDP range and set `TARNMEDIA_PUBLIC_IP` to the host's public
address, which is slower and easy to get subtly wrong. Host networking is the
supported path.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `TARNMEDIA_ADDR` | `:8088` | Public HTTP/WebSocket listener. |
| `TARNMEDIA_JWT_SECRET` | *(required, ≥32 chars)* | HS256 secret shared with your application. |
| `TARNMEDIA_ISSUER` | `tarnveil-server` | Expected `iss` claim. Set this to your own application's issuer. |
| `TARNMEDIA_CONTROL_ADDR` | `127.0.0.1:8089` | Control + metrics listener. Loopback only; startup fails otherwise. |
| `TARNMEDIA_CONTROL_SECRET` | *(required, ≥32 chars)* | Bearer secret for the control API and for outgoing session validation. |
| `TARNMEDIA_AUTH_URL` | `http://127.0.0.1:3001/internal/tarnmedia/validate` | Your session-validation endpoint (see below). Must be HTTPS or loopback HTTP. |
| `TARNMEDIA_ALLOWED_ORIGINS` | local dev origins | Exact scheme+host allowlist, comma-separated. |
| `TARNMEDIA_PUBLIC_IP` | *(empty)* | Address to advertise in ICE candidates when behind NAT. |
| `TARNMEDIA_UDP_MIN` / `TARNMEDIA_UDP_MAX` | `50000` / `50100` | Media UDP port range. |
| `TARNMEDIA_ICE_URLS` | *(empty)* | Your own STUN/TURN endpoints, comma-separated. No third-party server is used by default. |
| `TARNMEDIA_ICE_USERNAME` / `TARNMEDIA_ICE_CREDENTIAL` | *(empty)* | Credentials for the above. |
| `TARNMEDIA_MAX_PEERS_PER_ROOM` | `25` | Hard cap per room, 2–100. |

## Integrating your application

TarnMedia never talks to your database and has no concept of users, friends or
permissions. Your application decides who may join which room, and expresses that
decision as a short-lived JWT.

### 1. Issue a media token

Sign with HS256 using `TARNMEDIA_JWT_SECRET`. Required claims:

```json
{
  "iss": "your-app",
  "aud": "tarnmedia",
  "sub": "user-42",
  "userId": "user-42",
  "room": "voice-general",
  "participantId": "user-42__laptop",
  "sessionVersion": 7,
  "username": "alice",
  "displayName": "Alice",
  "avatarUrl": "https://…",
  "issuedAtMs": 1767225600000,
  "iat": 1767225600,
  "exp": 1767225900
}
```

Rules enforced on parse: `aud` must be exactly `tarnmedia`; `iss` must match
`TARNMEDIA_ISSUER`; `exp` is mandatory; `sub` must be non-empty and equal to
`userId`; `username` must be non-empty; `sessionVersion` must be ≥ 0;
`room` and `participantId` must be 1–180 characters without control characters.
Only HS256 is accepted. Keep the lifetime short — minutes, not hours.

`participantId` identifies one *device*, so the same user may join twice from two
devices. `sessionVersion` is the revocation counter described below.

### 2. Expose a session-validation endpoint

A signed JWT alone cannot be revoked before it expires, so on every WebSocket
authentication TarnMedia calls back into your application:

```
POST $TARNMEDIA_AUTH_URL
Authorization: Bearer $TARNMEDIA_CONTROL_SECRET
Content-Type: application/json

{"userId": "user-42", "sessionVersion": 7}
```

Reply **`204 No Content`** to admit the participant. Any other status rejects the
connection. This is where you check that the user still exists, is not banned, and
that their current `sessionVersion` still matches the one in the token — bump that
counter on password change or logout-everywhere and outstanding media tokens stop
working immediately.

The call is expected to be loopback or same-datacenter; if it fails, the
connection is refused (fails closed).

### 3. Signaling

Connect to `GET /v1/ws` and send authentication as the **first** message, so the
token never appears in a URL or a proxy access log:

```json
{"event":"authenticate","data":{"token":"<media JWT>"}}
```

Every frame is `{"event": "...", "data": {...}}`, where `data` is a JSON object,
not a JSON-encoded string.

Server → client: `authenticated`, `participants` (roster snapshots), `tracks`
(track-to-MID assignments), `offer`, `candidate`, `error`.
Client → server: `answer`, `candidate`, `state` (microphone/camera/screen flags).

TarnMedia is the offerer: it sends an `offer` whenever the set of forwarded tracks
changes, and expects your `answer`.

### 4. Control API (optional)

`POST /v1/control` on the loopback listener, `Authorization: Bearer $TARNMEDIA_CONTROL_SECRET`,
with `{"action": …}` — `closeRoom`, `evictUser`, or `revokeUser` (invalidate
already-issued tokens up to a timestamp). Responds `{"ok":true,"affectedPeers":N}`.
Use it to make a kick or ban in your application take effect on an in-progress
call. Never expose this listener publicly.

## Operating notes

- Expose the media UDP range directly. Proxy only the HTTP/WebSocket handler —
  putting media through a reverse proxy defeats the point.
- `/metrics` is on the loopback listener, not the public one. Scrape it locally.
- Before trusting a deployment, verify a call between two physical devices on
  different networks, including TURN fallback. NAT traversal is the part that
  breaks in the real world, and no unit test covers it.
- TURN credentials for browsers are generated by your application, not here.

## Development

```bash
go test ./...
go vet ./...
gofmt -l .
```

CI runs the same three on every push. See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Apache-2.0 — see [LICENSE](LICENSE). Pion and the other dependencies are MIT/BSD.
