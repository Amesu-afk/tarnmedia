//go:build !simulcastloss

package sfu

import "net/http"

// testLossControl is nil in normal builds. The `simulcastloss` build tag
// installs a loopback-only control endpoint and a packet-loss injector so the
// browser smoke harness can exercise real congestion without OS-level shapers.
var testLossControl http.Handler

func testDropForwardedPacket() bool { return false }
