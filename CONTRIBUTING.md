# Contributing

Thanks for looking. A few honest notes first.

TarnMedia is a small project with one maintainer, extracted from an application
that uses it in production. That shapes what gets merged: changes that make the
code smaller, clearer or more correct are welcome; changes that add surface area
I would then have to maintain forever usually are not.

**Open an issue before writing a large patch.** I would rather say "not a fit"
in a paragraph than have you spend a weekend on something I decline.

## Good contributions

- Bug fixes with a test that fails before and passes after.
- Documentation fixes — especially in the integration section of the README,
  since I am too close to it to notice what is missing.
- Portability and deployment fixes (NAT, ICE, containers) with a description of
  the setup you reproduced them on.
- Removing code.

## Probably not

- Recording, transcoding, SIP, or persistence. Out of scope on purpose.
- New dependencies, unless they replace more code than they add.
- Reformatting or renaming sweeps.
- Simulcast/SVC: wanted eventually, but it is a design conversation before it is
  a pull request. Open an issue.

## Before you push

```bash
gofmt -l .      # must print nothing
go vet ./...
go test ./...
```

CI runs exactly these. Keep the standard library and Pion idioms already used in
the file you are editing; match the surrounding style rather than introducing a
new one.

Commits: short imperative subject, and explain *why* in the body if it is not
obvious. Squash noise before opening the pull request.

## Tests

`internal/auth` and `internal/config` are cheap to test and should stay covered.
`internal/sfu` has tests that drive the real signaling path; if you change
signaling, extend them there rather than mocking the WebSocket away.

Nothing in the test suite exercises real NAT traversal — that is verified by hand
between two physical devices on different networks. If your change touches ICE,
say in the pull request how you verified it.

## Legal

By contributing you agree that your contribution is licensed under Apache-2.0,
the same as the project. There is no CLA.
