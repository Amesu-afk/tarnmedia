package sfu

import (
	"net"
	"testing"
)

// Every PeerConnection shares one ICE UDP mux instead of taking a port from a
// 100-port range. This is what removes the old ~100-peer process ceiling.
func TestServerUsesSingleUDPMux(t *testing.T) {
	server := testServer(t)
	if server.udpMux == nil {
		t.Fatal("expected an ICE UDP mux")
	}
	addresses := server.udpMux.GetListenAddresses()
	if len(addresses) == 0 {
		t.Fatal("expected at least one mux listener")
	}
	port := muxPort(t, addresses[0])
	for _, address := range addresses[1:] {
		if muxPort(t, address) != port {
			t.Fatalf("mux listener %s does not use the shared port %d", address, port)
		}
	}

	// Two servers constructed with UDPPort 0 must bind different ephemeral
	// ports: the fixed production port is a single listener, not a global range.
	other := testServer(t)
	if other.udpMux == nil {
		t.Fatal("expected an ICE UDP mux on the second server")
	}
	otherAddresses := other.udpMux.GetListenAddresses()
	if len(otherAddresses) == 0 {
		t.Fatal("expected at least one mux listener on the second server")
	}
	if muxPort(t, otherAddresses[0]) == port {
		t.Fatalf("two servers unexpectedly share mux port %d", port)
	}
}

func muxPort(t *testing.T, address net.Addr) int {
	t.Helper()
	udp, ok := address.(*net.UDPAddr)
	if !ok {
		t.Fatalf("unexpected mux address type %T", address)
	}
	return udp.Port
}
