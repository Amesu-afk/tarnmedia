package sfu

import (
	"testing"

	"github.com/pion/webrtc/v4"
)

// A track snapshot that arrives while SDP is busy used to be dropped silently:
// the subscriber heard nothing until some unrelated room event triggered a new
// sync. The peer now remembers it so the answer handler can repeat the sync.
func TestSyncPeerRemembersChangeWhileNegotiating(t *testing.T) {
	api := webrtc.NewAPI()
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if pc.SignalingState() != webrtc.SignalingStateHaveLocalOffer {
		t.Fatalf("unexpected signaling state %v", pc.SignalingState())
	}

	currentPeer := &peer{id: "p", pc: pc}
	currentRoom := &room{
		id:      "r",
		peers:   map[string]*peer{"p": currentPeer},
		tracks:  map[string]localTrack{},
		uplinks: map[string]localTrack{},
	}
	if err := currentRoom.syncPeer(currentPeer, map[string]localTrack{}); err != nil {
		t.Fatal(err)
	}
	if !currentPeer.syncDirty {
		t.Fatal("a track change dropped during negotiation must be remembered")
	}
}
