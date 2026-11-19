package sfu

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

func testLayerLimits() []layerLimit {
	return []layerLimit{
		{rid: "q", bitrate: 150_000},
		{rid: "h", bitrate: 400_000},
		{rid: "f", bitrate: 1_200_000},
	}
}

func TestSimulcastSourceGroupsRIDLayersUnderOneLogicalTrack(t *testing.T) {
	source := newSimulcastSource(&peer{id: "pub"}, webrtc.MimeTypeVP8)
	source.addLayer("q", 10, nil)
	source.addLayer("h", 20, nil)
	source.addLayer("f", 30, nil)
	if got := len(source.layerRIDs()); got != 3 {
		t.Fatalf("layers = %d, want 3", got)
	}
	if source.layer("h").ssrc != 20 {
		t.Fatalf("h layer ssrc = %d, want 20", source.layer("h").ssrc)
	}

	// A stale reader must not evict the replacement of the same RID.
	if source.removeLayer("h", 999) {
		t.Fatal("a stale layer must not remove the current one")
	}
	if source.layer("h") == nil {
		t.Fatal("the current h layer disappeared")
	}
	if source.removeLayer("h", 20) {
		t.Fatal("the source is not empty while q and f are alive")
	}
	if source.layer("h") != nil {
		t.Fatal("h was removed")
	}
	source.removeLayer("q", 10)
	if !source.removeLayer("f", 30) {
		t.Fatal("the source must be empty after all layers are gone")
	}
}

func TestSimulcastSourceExcludesSilentLayers(t *testing.T) {
	source := newSimulcastSource(&peer{id: "pub"}, webrtc.MimeTypeVP8)
	source.addLayer("q", 10, nil)
	source.addLayer("h", 20, nil)
	source.addLayer("f", 30, nil)
	source.layer("f").lastPacket.Store(time.Now().Add(-simulcastLayerSilence - time.Second).UnixNano())

	rids := source.layerRIDs()
	if len(rids) != 2 {
		t.Fatalf("active layers = %v, want q and h", rids)
	}
	for _, rid := range rids {
		if rid == "f" {
			t.Fatal("a silent layer must not be offered for selection")
		}
	}

	// A resumed encoding is eligible again without a new OnTrack.
	source.layer("f").lastPacket.Store(time.Now().UnixNano())
	if got := len(source.layerRIDs()); got != 3 {
		t.Fatalf("active layers after resume = %d, want 3", got)
	}
}

func TestSubscriberReportsDriveLayerSelection(t *testing.T) {
	start := time.Now().Add(-3 * time.Second)

	t.Run("loss degrades", func(t *testing.T) {
		selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "f", start)
		selector.accept("f", true, start)
		sink := &subscriberSink{selector: selector, video: true}
		metrics := &serverMetrics{}

		packets, err := rtcp.Unmarshal(mustMarshalRTCP(t,
			&rtcp.ReceiverReport{Reports: []rtcp.ReceptionReport{{SSRC: 42, FractionLost: 51}}}, // 20 %
		))
		if err != nil {
			t.Fatal(err)
		}
		applySubscriberRTCP(packets, sink, metrics)

		target, ok := selector.nextSwitch(time.Now())
		if !ok || target != "h" {
			t.Fatalf("target after 20%% loss = %q, ok = %t; want h", target, ok)
		}
	})

	t.Run("remb degrades to budget", func(t *testing.T) {
		selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "f", start)
		selector.accept("f", true, start)
		sink := &subscriberSink{selector: selector, video: true}
		metrics := &serverMetrics{}

		packets, err := rtcp.Unmarshal(mustMarshalRTCP(t,
			&rtcp.ReceiverEstimatedMaximumBitrate{Bitrate: 300_000},
		))
		if err != nil {
			t.Fatal(err)
		}
		applySubscriberRTCP(packets, sink, metrics)

		if metrics.rembReceived.Load() != 1 {
			t.Fatalf("REMB counter = %d, want 1", metrics.rembReceived.Load())
		}
		target, ok := selector.nextSwitch(time.Now())
		if !ok || target != "q" {
			t.Fatalf("target after 300 kbit/s REMB = %q, ok = %t; want q", target, ok)
		}
	})
}

func mustMarshalRTCP(t *testing.T, packets ...rtcp.Packet) []byte {
	t.Helper()
	raw, err := rtcp.Marshal(packets)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestSimulcastMetricsExposeLayerAndKeyFrameWait(t *testing.T) {
	now := time.Now()
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "h", now)
	selector.accept("h", true, now)
	server := &Server{
		rooms: map[string]*room{
			"room": {
				id: "room",
				subscriptions: map[string]map[string]*subscriberSink{
					"track": {"peer": {selector: selector}},
				},
			},
		},
	}
	scrape := func() string {
		recorder := httptest.NewRecorder()
		server.handleMetrics(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
		return recorder.Body.String()
	}

	body := scrape()
	for _, want := range []string{
		`tarnmedia_simulcast_layer{layer="h"} 1`,
		`tarnmedia_simulcast_layer{layer="q"} 0`,
		"tarnmedia_simulcast_waiting_keyframe 0",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}

	selector.beginSwitch("f", now)
	selector.mu.Lock()
	selector.waitingSince = time.Now().Add(-2 * time.Second)
	selector.mu.Unlock()
	body = scrape()
	if !strings.Contains(body, "tarnmedia_simulcast_waiting_keyframe 1") {
		t.Fatalf("waiting gauge missing:\n%s", body)
	}
	waitLine := ""
	for _, line := range strings.Split(body, "\n") {
		if strings.HasPrefix(line, "tarnmedia_simulcast_wait_seconds ") {
			waitLine = line
		}
	}
	var seconds float64
	if _, err := fmt.Sscanf(waitLine, "tarnmedia_simulcast_wait_seconds %f", &seconds); err != nil || seconds < 1.9 {
		t.Fatalf("wait gauge = %q, want about 2 seconds", waitLine)
	}
}

func TestRTPRewriterKeepsStreamContinuousAcrossLayers(t *testing.T) {
	rewriter := newRTPRewriter(0xFFFF) // sequence starts at 65535, so wrap is covered
	first := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 111, SequenceNumber: 100, Timestamp: 9000},
		Payload: []byte{1},
	}
	rewriter.rewrite(first)
	if first.SSRC != 0xFFFF {
		t.Fatalf("outgoing SSRC = %d, want 65535", first.SSRC)
	}
	if first.SequenceNumber != 0xFFFF {
		t.Fatalf("first sequence = %d, want 65535", first.SequenceNumber)
	}
	if first.Timestamp != 9000 {
		t.Fatalf("first timestamp = %d, want 9000", first.Timestamp)
	}

	second := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 111, SequenceNumber: 101, Timestamp: 12000},
		Payload: []byte{2},
	}
	rewriter.rewrite(second)
	if second.SequenceNumber != 0 {
		t.Fatalf("sequence must wrap continuously, got %d", second.SequenceNumber)
	}
	if second.Timestamp != 12000 {
		t.Fatalf("timestamp inside one layer changed: %d", second.Timestamp)
	}

	// The next layer has its own SSRC, sequence and a timestamp clock that is
	// unrelated to the previous encoder's.
	rewriter.beginLayerSwitch()
	switched := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 999, SequenceNumber: 5000, Timestamp: 900_000},
		Payload: []byte{3},
	}
	rewriter.rewrite(switched)
	if switched.SSRC != 0xFFFF {
		t.Fatalf("SSRC after a switch = %d, want 65535", switched.SSRC)
	}
	if switched.SequenceNumber != 1 {
		t.Fatalf("sequence after a switch = %d, want 1", switched.SequenceNumber)
	}
	if want := uint32(15000); switched.Timestamp != want {
		t.Fatalf("timestamp after a switch = %d, want %d (one frame after the last output)", switched.Timestamp, want)
	}

	next := &rtp.Packet{
		Header:  rtp.Header{Version: 2, SSRC: 999, SequenceNumber: 5001, Timestamp: 903_000},
		Payload: []byte{4},
	}
	rewriter.rewrite(next)
	if next.SequenceNumber != 2 {
		t.Fatalf("sequence did not continue after the switch: %d", next.SequenceNumber)
	}
	if want := uint32(18000); next.Timestamp != want {
		t.Fatalf("timestamp = %d, want %d", next.Timestamp, want)
	}
}

func TestLayerSwapWaitsForTargetKeyFrame(t *testing.T) {
	now := time.Unix(1000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "h", now)
	selector.accept("h", true, now)

	selector.setREMB(300_000) // below h, enough for q
	target, ok := selector.nextSwitch(now)
	if !ok || target != "q" {
		t.Fatalf("target = %q, ok = %t; want q", target, ok)
	}

	// A delta packet of the target layer must not be forwarded: the decoder has
	// no reference frame for it yet.
	if send, _ := selector.accept("q", false, now); send {
		t.Fatal("a delta packet of the target layer must wait for a key frame")
	}
	// The current layer keeps flowing while the switch waits, so the picture
	// does not freeze.
	if send, switched := selector.accept("h", false, now); !send || switched {
		t.Fatalf("current layer = %t/%t, want true/false", send, switched)
	}
	if send, switched := selector.accept("q", true, now); !send || !switched {
		t.Fatalf("key frame = %t/%t, want true/true", send, switched)
	}
	if selector.priorityRID() != "q" {
		t.Fatalf("priority after the switch = %q, want q", selector.priorityRID())
	}
}

func TestLayerSelectorStartsAtHighAndGrowsOnlyAfterStableWindow(t *testing.T) {
	now := time.Unix(2000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "h", now)
	if selector.current != "h" {
		t.Fatalf("initial layer = %q, want h", selector.current)
	}
	selector.setREMB(2_000_000) // enough for f
	if _, ok := selector.nextSwitch(now.Add(3 * time.Second)); ok {
		t.Fatal("growth before the stable window must be refused")
	}
	target, ok := selector.nextSwitch(now.Add(simulcastGrowAfter))
	if !ok || target != "f" {
		t.Fatalf("target after the stable window = %q, ok = %t; want f", target, ok)
	}
}

func TestLayerSelectorDropsImmediatelyAndRespectsHysteresis(t *testing.T) {
	now := time.Unix(3000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "f", now)
	selector.accept("f", true, now)
	selector.setREMB(300_000)
	target, ok := selector.nextSwitch(now)
	if !ok || target != "q" {
		t.Fatalf("degradation target = %q, ok = %t; want q", target, ok)
	}
	selector.accept("q", true, now)

	// A second switch cannot happen inside the guard window.
	selector.setREMB(2_000_000)
	if _, ok := selector.nextSwitch(now.Add(time.Second)); ok {
		t.Fatal("a second switch inside the guard window must be refused")
	}
}

func TestLayerSelectorFallsBackWhenTheCurrentLayerDisappears(t *testing.T) {
	now := time.Unix(4000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "h", now)
	selector.accept("h", true, now)

	selector.setAvailable([]string{"q", "f"})
	fallbackAt := now.Add(simulcastPreferredWait + time.Millisecond)
	target, ok := selector.nextSwitch(fallbackAt)
	if !ok || target != "q" {
		t.Fatalf("fallback = %q, ok = %t; want q", target, ok)
	}
	// Even a layer that vanished right after a switch is replaced without
	// waiting for the guard: the current encoding is not producing packets.
	if send, _ := selector.accept("q", true, fallbackAt); !send {
		t.Fatal("q key frame must be forwarded after the fallback")
	}
}

func TestLayerSelectorRampsFromEarlyLowLayerToStartLayer(t *testing.T) {
	now := time.Unix(4500, 0)
	// Only q is on the wire when the sink is created. The selector waits for h
	// instead of forwarding q and switching up immediately.
	selector := newLayerSelector(testLayerLimits(), []string{"q"}, "h", now)
	if selector.current != "h" {
		t.Fatalf("initial layer = %q, want h", selector.current)
	}
	if send, _ := selector.accept("q", true, now.Add(100*time.Millisecond)); send {
		t.Fatal("q must not be forwarded while the preferred layer is pending")
	}
	selector.setAvailable([]string{"q", "h"})
	if send, switched := selector.accept("h", true, now.Add(150*time.Millisecond)); !send || switched {
		t.Fatalf("h = %t/%t, want true/false", send, switched)
	}
}

func TestLayerSelectorFallsBackWhenThePreferredLayerNeverArrives(t *testing.T) {
	now := time.Unix(4550, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q"}, "h", now)
	if _, ok := selector.nextSwitch(now.Add(simulcastPreferredWait - time.Millisecond)); ok {
		t.Fatal("the preferred layer wait expired too early")
	}
	target, ok := selector.nextSwitch(now.Add(simulcastPreferredWait + time.Millisecond))
	if !ok || target != "q" {
		t.Fatalf("fallback target = %q, ok = %t; want q", target, ok)
	}
}

func TestLayerSelectorGrowsOnLosslessReportsWithoutREMB(t *testing.T) {
	now := time.Unix(6000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "h", now)
	selector.accept("h", true, now)
	// Chrome may not send REMB; a lossless receiver report is the growth signal.
	selector.noteReport(0, now)
	if _, ok := selector.nextSwitch(now.Add(4 * time.Second)); ok {
		t.Fatal("growth before the stable window must be refused")
	}
	target, ok := selector.nextSwitch(now.Add(simulcastGrowAfter))
	if !ok || target != "f" {
		t.Fatalf("target = %q, ok = %t; want f", target, ok)
	}
	if send, switched := selector.accept("f", true, now.Add(simulcastGrowAfter)); !send || !switched {
		t.Fatal("f key frame must complete the growth")
	}

	// Real loss must degrade immediately (after the 2 s guard) and remember
	// that f was too big.
	selector.noteReport(0.10, now.Add(9*time.Second))
	target, ok = selector.nextSwitch(now.Add(11 * time.Second))
	if !ok || target != "h" {
		t.Fatalf("degradation target = %q, ok = %t; want h", target, ok)
	}
	selector.accept("h", true, now.Add(11*time.Second))
	selector.noteReport(0, now.Add(12*time.Second))
	// Even with a long lossless window f stays out until the cooldown.
	if target, ok = selector.nextSwitch(now.Add(30 * time.Second)); ok {
		t.Fatalf("f was retried during the cooldown: %q", target)
	}
	if target, ok = selector.nextSwitch(now.Add(simulcastLayerCooldown + 12*time.Second)); !ok || target != "f" {
		t.Fatalf("after cooldown target = %q, ok = %t; want f", target, ok)
	}
}

func TestLayerSelectorUsesREMBBudget(t *testing.T) {
	now := time.Unix(5000, 0)
	selector := newLayerSelector(testLayerLimits(), []string{"q", "h", "f"}, "q", now)
	selector.accept("q", true, now)

	// 500 kbit/s budget 450k fits h (400k), not f.
	selector.setREMB(500_000)
	selector.stableSince = now.Add(-simulcastGrowAfter)
	target, ok := selector.nextSwitch(now)
	if !ok || target != "h" {
		t.Fatalf("target = %q, ok = %t; want h", target, ok)
	}
}

func TestKeyFrameDetection(t *testing.T) {
	tests := []struct {
		name    string
		mime    string
		payload []byte
		want    bool
	}{
		{
			name: "vp8 key frame",
			mime: webrtc.MimeTypeVP8,
			// S=1, PID=0, then payload header P=0.
			payload: []byte{0x10, 0x00, 0x01},
			want:    true,
		},
		{
			name:    "vp8 delta frame",
			mime:    webrtc.MimeTypeVP8,
			payload: []byte{0x10, 0x01, 0x02},
			want:    false,
		},
		{
			name:    "vp8 continuation partition",
			mime:    webrtc.MimeTypeVP8,
			payload: []byte{0x00, 0x00},
			want:    false,
		},
		{
			name: "vp8 key frame with picture id",
			mime: webrtc.MimeTypeVP8,
			// X=1, S=1, PID=0; I=1; one-byte picture id; P=0.
			payload: []byte{0x90, 0x80, 0x07, 0x00},
			want:    true,
		},
		{
			name:    "vp9 key frame",
			mime:    webrtc.MimeTypeVP9,
			payload: []byte{0x08, 0x00},
			want:    true,
		},
		{
			name:    "vp9 delta frame",
			mime:    webrtc.MimeTypeVP9,
			payload: []byte{0x48, 0x00},
			want:    false,
		},
		{
			name:    "h264 idr",
			mime:    webrtc.MimeTypeH264,
			payload: []byte{0x00, 0x00, 0x00, 0x01, 0x65, 0x88},
			want:    true,
		},
		{
			name:    "h264 non-idr",
			mime:    webrtc.MimeTypeH264,
			payload: []byte{0x00, 0x00, 0x01, 0x41, 0x9a},
			want:    false,
		},
		{
			name:    "unknown codec is always decodable",
			mime:    "video/av1",
			payload: []byte{0x00},
			want:    true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			packet := &rtp.Packet{Payload: test.payload}
			if got := isKeyFrame(packet, test.mime); got != test.want {
				t.Fatalf("isKeyFrame = %t, want %t", got, test.want)
			}
		})
	}
}

// testSignalSocket returns the server side of a real websocket pair. peer.write
// needs a live connection, and taking one from httptest keeps the test from
// growing a fake with its own behavior.
func testSignalSocket(t *testing.T) *websocket.Conn {
	t.Helper()
	serverConn := make(chan *websocket.Conn, 1)
	httpServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		serverConn <- conn
	}))
	t.Cleanup(httpServer.Close)
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(httpServer.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	conn := <-serverConn
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// A publishing peer must not receive a downstream offer before its own publish
// answer is on the wire: the two offers would cross.
func TestPublisherGetsNoDownstreamOfferBeforePublishAnswer(t *testing.T) {
	pc, err := webrtc.NewAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	currentPeer := &peer{id: "subscriber", pc: pc, publisher: true, publishReady: false, ws: testSignalSocket(t)}
	currentRoom := &room{
		id: "r", simulcast: true, layers: testLayerLimits(),
		peers:         map[string]*peer{"subscriber": currentPeer},
		uplinks:       map[string]localTrack{},
		tracks:        map[string]localTrack{},
		subscriptions: map[string]map[string]*subscriberSink{},
		metrics:       &serverMetrics{},
	}
	candidate := localTrack{
		ownerID: "publisher", source: "microphone",
		track: testTrack(t, "publisher:microphone:1"),
	}
	available := map[string]localTrack{"publisher:microphone:1": candidate}

	if err := currentRoom.syncPeer(currentPeer, available); err != nil {
		t.Fatal(err)
	}
	if !currentPeer.syncDirty {
		t.Fatal("the pending track snapshot must be remembered")
	}
	if pc.LocalDescription() != nil {
		t.Fatal("a downstream offer was created during the publish handshake")
	}

	currentPeer.publishReady = true
	if err := currentRoom.syncPeer(currentPeer, available); err != nil {
		t.Fatal(err)
	}
	if pc.LocalDescription() == nil {
		t.Fatal("the downstream offer must follow the publish answer")
	}
}

// After the publish handshake the transport is already established. With no
// room tracks there is nothing to offer, and an empty offer must not be sent.
func TestPublisherWithNoTracksSendsNoEmptyOffer(t *testing.T) {
	pc, err := webrtc.NewAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	currentPeer := &peer{id: "publisher", pc: pc, publisher: true, publishReady: true}
	currentRoom := &room{
		id:            "r",
		peers:         map[string]*peer{"publisher": currentPeer},
		uplinks:       map[string]localTrack{},
		tracks:        map[string]localTrack{},
		subscriptions: map[string]map[string]*subscriberSink{},
		metrics:       &serverMetrics{},
	}
	if err := currentRoom.syncPeer(currentPeer, map[string]localTrack{}); err != nil {
		t.Fatal(err)
	}
	if pc.LocalDescription() != nil {
		t.Fatal("an empty downstream offer was created")
	}
}
