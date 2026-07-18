# TarnMedia

TarnMedia is TarnVeil's own WebRTC SFU service. It uses Pion for the standards
implementation, while room authorization, signaling, RTP forwarding, limits and
deployment remain part of this repository.

The first slice supports:

- authenticated, room-scoped WebSocket signaling at `GET /v1/ws`;
- one Opus audio track plus camera and screen video inputs per participant;
- forwarding of browser-negotiated standard WebRTC codecs (Opus, VP8, H.264 and
  other codecs supported by both endpoints) without transcoding;
- trickle ICE, renegotiation, periodic video keyframe requests and exact Origin
  allowlisting;
- a bounded UDP port range and optional public-IP/STUN/TURN configuration;
- health and readiness endpoints.

It is not enabled for users yet. `MEDIA_BACKEND=livekit` remains the safe default
until the TarnMedia browser transport and a two-client call test are complete.

## Local run

Set the variables from `.env.example`, using the same
`TARNMEDIA_JWT_SECRET` in `server/.env` and this process, then run:

```powershell
$env:TARNMEDIA_JWT_SECRET = 'a-random-secret-at-least-32-characters-long'
& 'C:\Program Files\Go\bin\go.exe' run ./cmd/tarnmedia
```

The HTTP service listens on `:8088` and WebRTC media uses UDP `50000-50100` by
default. Production must expose the UDP range directly and proxy `/v1/ws` with
WebSocket support. TURN should be configured before enabling the backend for
users behind restrictive networks.

## Signaling contract

The client sends authentication as its first message so the short-lived token is
not placed in a URL:

```json
{"event":"authenticate","data":{"token":"<media JWT>"}}
```

The server then sends `authenticated`, WebRTC `offer`, and `candidate` events.
The client returns `answer` and `candidate` events. The `data` field is a JSON
object, not a JSON-encoded string.

## Next slice

1. Add the browser `TarnMediaTransport` behind the media backend flag.
2. Add an authenticated internal control endpoint so the main server can evict a
   user or close a room when a call grant is revoked.
3. Pass a two-browser audio/camera/screen test, including reconnect and device
   switching.
4. Add simulcast layer selection, congestion metrics, TURN and load tests.
5. Only then switch a small beta cohort from LiveKit to TarnMedia.
