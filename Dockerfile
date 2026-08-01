# Build
FROM golang:1.24-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags='-s -w' -o /out/tarnmedia ./cmd/tarnmedia

# Run
FROM alpine:3.20
RUN adduser -D -u 10001 tarnmedia
COPY --from=build /out/tarnmedia /usr/local/bin/tarnmedia
USER tarnmedia

# Public signaling. Media uses UDP 50000-50100 and the control API stays on
# loopback, so run with --network host: see the Docker section of the README.
EXPOSE 8088

ENTRYPOINT ["/usr/local/bin/tarnmedia"]
