package sfu

import (
	"testing"
	"time"

	"github.com/pion/webrtc/v4"
)

func testTrack(t *testing.T, id string) *webrtc.TrackLocalStaticRTP {
	t.Helper()
	track, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus}, id, "stream",
	)
	if err != nil {
		t.Fatal(err)
	}
	return track
}

// A full subscriber queue must drop, not block the publisher's broadcast loop.
func TestBroadcastDoesNotBlockOnFullSubscriberQueue(t *testing.T) {
	r := &room{
		id:            "room",
		subscriptions: make(map[string]map[string]*subscriberSink),
		metrics:       &serverMetrics{},
	}
	sink := &subscriberSink{
		room: r, trackID: "track", peerID: "peer",
		queue: make(chan []byte, 1), done: make(chan struct{}),
	}
	sink.queue <- []byte{1} // queue is full
	r.subscriptions["track"] = map[string]*subscriberSink{"peer": sink}

	done := make(chan struct{})
	go func() {
		r.broadcast("track", []byte{2, 3, 4})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked on a full subscriber queue")
	}

	// The queued packet is still there and the sink is alive.
	select {
	case packet := <-sink.queue:
		if len(packet) != 1 || packet[0] != 1 {
			t.Fatalf("unexpected queued packet %v", packet)
		}
	default:
		t.Fatal("expected the original packet to remain queued")
	}
}

func TestEnsureSinkIsSharedPerSubscriber(t *testing.T) {
	r := &room{
		id:            "room",
		subscriptions: make(map[string]map[string]*subscriberSink),
		metrics:       &serverMetrics{},
	}
	currentPeer := &peer{id: "peer-1"}
	candidate := localTrack{ownerID: "publisher", source: "camera", track: testTrack(t, "track-id")}

	first, err := r.ensureSink(currentPeer, candidate)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.ensureSink(currentPeer, candidate)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatal("expected one sink per (track, subscriber)")
	}
	if cap(first.queue) != subscriberVideoQueue {
		t.Fatalf("video queue size = %d, want %d", cap(first.queue), subscriberVideoQueue)
	}

	r.dropSink("track-id", "peer-1", first)
	if _, exists := r.subscriptions["track-id"]; exists {
		t.Fatal("track subscription should be removed with its last sink")
	}
	select {
	case <-first.done:
	default:
		t.Fatal("dropped sink must be closed")
	}
}

func TestDropPeerSinksRemovesEveryTrack(t *testing.T) {
	r := &room{
		id:            "room",
		subscriptions: make(map[string]map[string]*subscriberSink),
		metrics:       &serverMetrics{},
	}
	for _, trackID := range []string{"a", "b"} {
		sink := &subscriberSink{
			room: r, trackID: trackID, peerID: "peer-1",
			queue: make(chan []byte, 1), done: make(chan struct{}),
		}
		r.subscriptions[trackID] = map[string]*subscriberSink{"peer-1": sink}
	}
	r.dropPeerSinks("peer-1")
	if len(r.subscriptions) != 0 {
		t.Fatalf("expected all subscriptions removed, got %d tracks", len(r.subscriptions))
	}
}
