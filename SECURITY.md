# Security Policy

## Scope and expectations

TarnMedia is maintained by one person in their spare time. Please read this
before reporting, so your expectations are accurate:

- I will acknowledge a report within **14 days**.
- There is **no bounty**, no CVE coordination service, and no guaranteed fix
  timeline. Serious issues in the authentication, authorization or signaling
  path get priority; everything else is best-effort.
- If I cannot fix something, I will say so and document it in the README rather
  than leave it silently unaddressed.

If that is not good enough for your use case, that is a reasonable conclusion —
see the Status section of the README.

## Reporting

Use GitHub's **private vulnerability reporting** (the "Report a vulnerability"
button on the Security tab). It keeps the report private until a fix exists and
does not require exposing an email address.

Please do not open a public issue for anything that lets an unauthorized party
join a room, read media, impersonate a participant, or crash the service.

Useful reports include: the version or commit, configuration relevant to the
issue, and the smallest sequence of steps that demonstrates it.

## Supported versions

Only the current `main` branch. There are no maintained release branches and no
backports.

## What is in scope

- Bypassing JWT verification, the `Origin` allowlist, or room authorization.
- Escaping a room: receiving or publishing media in a room the token does not
  grant.
- Making session revocation (`sessionVersion`, the control API) ineffective.
- Remote crashes or unbounded resource use reachable before authentication.

## What is out of scope

- Anything requiring the operator's own `TARNMEDIA_JWT_SECRET` or
  `TARNMEDIA_CONTROL_SECRET`. Those are trusted by design.
- Consequences of exposing the control listener publicly. It is loopback-only by
  default and startup fails if configured otherwise.
- Denial of service by a legitimately authorized participant sending expensive
  but valid media. Per-room caps exist, but a hostile authorized peer is not part
  of the threat model.
- Reports from automated scanners without a demonstrated impact.

## Known limitations

These are design gaps, not vulnerabilities, and are already documented:

- **Media is not end-to-end encrypted.** Traffic is DTLS-SRTP encrypted between
  each peer and the SFU, and the SFU necessarily sees the plaintext RTP in order
  to forward it. An operator who controls the server can access call media. If
  your threat model excludes the server operator, TarnMedia is the wrong tool.
- No simulcast/SVC and no load-tested capacity numbers.
