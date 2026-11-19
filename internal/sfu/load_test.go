//go:build loadtest

package sfu

// Load test for one simulcast publisher and 24 subscribers in one room.
//
// This test is deliberately behind a build tag: it opens 25 real
// PeerConnections, sends ~30 Mbit/s of RTP through the SFU and therefore is not
// part of `go test ./...`.
//
//	go test -tags loadtest ./internal/sfu -run TestLoad25Peers -v -count=1
//
// The SFU and the synthetic Pion clients share one process, so the CPU number
// is an upper bound for the server: it includes the clients' SRTP and RTP work.
// Bandwidth, forwarded bytes, subscriber packets and queue drops are measured
// independently.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	rmetrics "runtime/metrics"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

const (
	loadRoom        = "load-room"
	loadPeerCount   = 25
	loadSubscribers = loadPeerCount - 1
	loadJWTSecret   = "load-test-jwt-secret-long-enough-for-real"
)

func loadSecret() string {
	if secret := strings.TrimSpace(os.Getenv("TARNMEDIA_JWT_SECRET")); secret != "" {
		return secret
	}
	return loadJWTSecret
}

type loadPeer struct {
	id     string
	pc     *webrtc.PeerConnection
	ws     *websocket.Conn
	writeM sync.Mutex

	authenticated chan struct{}
	authOnce      sync.Once
	publishAnswer chan struct{}
	publishOnce   sync.Once

	answers atomic.Uint64
	bytes   atomic.Uint64
	packets atomic.Uint64
}

func (p *loadPeer) send(event string, data any) error {
	p.writeM.Lock()
	defer p.writeM.Unlock()
	return p.ws.WriteJSON(map[string]any{"event": event, "data": data})
}

func (p *loadPeer) readSignals(ctx context.Context) {
	for {
		_, raw, err := p.ws.ReadMessage()
		if err != nil {
			return
		}
		message := signalMessage{}
		if err := json.Unmarshal(raw, &message); err != nil {
			return
		}
		switch message.Event {
		case "authenticated":
			p.authOnce.Do(func() { close(p.authenticated) })
		case "publish_answer":
			answer := webrtc.SessionDescription{}
			if err := json.Unmarshal(message.Data, &answer); err != nil {
				return
			}
			if err := p.pc.SetRemoteDescription(answer); err != nil {
				return
			}
			p.publishOnce.Do(func() { close(p.publishAnswer) })
		case "offer":
			offer := webrtc.SessionDescription{}
			if err := json.Unmarshal(message.Data, &offer); err != nil {
				return
			}
			if err := p.pc.SetRemoteDescription(offer); err != nil {
				return
			}
			answer, err := p.pc.CreateAnswer(nil)
			if err != nil {
				return
			}
			if err := p.pc.SetLocalDescription(answer); err != nil {
				return
			}
			if err := p.send("answer", p.pc.LocalDescription()); err != nil {
				return
			}
			p.answers.Add(1)
		case "candidate":
			candidate := webrtc.ICECandidateInit{}
			if err := json.Unmarshal(message.Data, &candidate); err != nil {
				return
			}
			if err := p.pc.AddICECandidate(candidate); err != nil {
				return
			}
		case "error":
			// A transient signaling conflict is retried by the caller; keeping
			// the read loop alive is what matters for the measurement.
		}
	}
}

func (p *loadPeer) close() {
	_ = p.ws.Close()
	_ = p.pc.Close()
}

func newLoadPeer(t *testing.T, ctx context.Context, wsURL, id string, publish bool) *loadPeer {
	t.Helper()
	pc, err := webrtc.NewAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	peer := &loadPeer{
		id: id, pc: pc,
		authenticated: make(chan struct{}),
		publishAnswer: make(chan struct{}),
	}
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		_ = peer.send("candidate", candidate.ToJSON())
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		go func() {
			for {
				packet, _, err := track.ReadRTP()
				if err != nil {
					return
				}
				peer.packets.Add(1)
				peer.bytes.Add(uint64(packet.MarshalSize()))
			}
		}()
	})

	header := http.Header{"Origin": []string{"https://tarnveil.ru"}}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		_ = pc.Close()
		t.Fatalf("dial %s: %v", id, err)
	}
	peer.ws = ws
	go peer.readSignals(ctx)
	payload := map[string]any{"token": loadToken(t, id)}
	if publish {
		payload["publish"] = true
		payload["simulcast"] = true
	}
	if err := peer.send("authenticate", payload); err != nil {
		peer.close()
		t.Fatalf("authenticate %s: %v", id, err)
	}
	select {
	case <-peer.authenticated:
	case <-time.After(10 * time.Second):
		peer.close()
		t.Fatalf("%s did not authenticate", id)
	}
	return peer
}

func loadToken(t *testing.T, id string) string {
	t.Helper()
	claims := auth.Claims{
		Room: loadRoom, ParticipantID: id, UserID: "user-" + id,
		SessionVersion: 1, Username: id, DisplayName: id,
		IssuedAtMS: time.Now().UnixMilli(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: auth.DefaultIssuer, Subject: "user-" + id,
			Audience:  jwt.ClaimStrings{auth.Audience},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(loadSecret()))
	if err != nil {
		t.Fatal(err)
	}
	return token
}

type loadPublisher struct {
	streams []loadLayerStream
	sent    atomic.Uint64
	sender  *webrtc.RTPSender
	video   *webrtc.RTPTransceiver
}

type loadLayerStream struct {
	track  *webrtc.TrackLocalStaticRTP
	rid    string
	bytes  int
	seq    uint16
	ts     uint32
	frames uint32
}

// addSimulcastTracks builds one video m-line with three RID encodings, as the
// browser does with sendEncodings.
func addSimulcastTracks(t *testing.T, pc *webrtc.PeerConnection) *loadPublisher {
	t.Helper()
	publisher := &loadPublisher{}
	first, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "camera", "publisher",
		webrtc.WithRTPStreamID("q"),
	)
	if err != nil {
		t.Fatal(err)
	}
	transceiver, err := pc.AddTransceiverFromTrack(first, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Per-frame payload at 30 fps: 150/400/1200 kbit/s.
	publisher.streams = append(publisher.streams, loadLayerStream{
		track: first, rid: "q", bytes: 150_000 / 8 / 30,
	})
	for _, layer := range []struct {
		rid   string
		bits  int
		track *webrtc.TrackLocalStaticRTP
	}{
		{rid: "h", bits: 400_000},
		{rid: "f", bits: 1_200_000},
	} {
		track, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "camera", "publisher",
			webrtc.WithRTPStreamID(layer.rid),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := transceiver.Sender().AddEncoding(track); err != nil {
			t.Fatal(err)
		}
		publisher.streams = append(publisher.streams, loadLayerStream{
			track: track, rid: layer.rid, bytes: layer.bits / 8 / 30,
		})
	}
	publisher.sender = transceiver.Sender()
	publisher.video = transceiver
	return publisher
}

// runRTP sends one ~1.2 kB slice per packet, marks the first slice of every 30th
// frame as a VP8 key frame and advances sequence/timestamp as a real encoder.
// Pion does not stamp simulcast RIDs by itself, so the negotiated MID and RID
// header extensions are added explicitly, as a browser's RTP sender does.
func (p *loadPublisher) runRTP(ctx context.Context) {
	mid := p.video.Mid()
	parameters := p.sender.GetParameters()
	var midID, ridID uint8
	for _, extension := range parameters.HeaderExtensions {
		switch extension.URI {
		case "urn:ietf:params:rtp-hdrext:sdes:mid":
			midID = uint8(extension.ID)
		case "urn:ietf:params:rtp-hdrext:sdes:rtp-stream-id":
			ridID = uint8(extension.ID)
		}
	}
	if midID == 0 || ridID == 0 {
		// Without these extensions Pion's receiver cannot attribute SSRCs to
		// RIDs and the SFU would never see a layer. Fail loudly.
		panic("publisher did not negotiate MID/RID header extensions")
	}
	for index := range p.streams {
		stream := &p.streams[index]
		go func() {
			ticker := time.NewTicker(time.Second / 30)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					stream.frames++
					keyFrame := stream.frames%30 == 1
					remaining := stream.bytes
					first := true
					for remaining > 0 {
						size := remaining
						if size > 1100 {
							size = 1100
						}
						remaining -= size
						payload := make([]byte, size)
						if first {
							// VP8 payload descriptor: S=1, PID=0, P=0 on an
							// intra frame and P=1 on a delta frame.
							payload[0] = 0x10
							if !keyFrame {
								payload[0] = 0x11
							}
							payload[1] = 0x00
							first = false
						}
						packet := &rtp.Packet{
							Header: rtp.Header{
								Version:        2,
								PayloadType:    96,
								SequenceNumber: stream.seq,
								Timestamp:      stream.ts,
								SSRC:           uint32(index + 1),
								Marker:         remaining == 0,
							},
							Payload: payload,
						}
						packet.Header.Extension = true
						packet.Header.ExtensionProfile = 0x1000
						_ = packet.Header.SetExtension(midID, []byte(mid))
						_ = packet.Header.SetExtension(ridID, []byte(stream.rid))
						stream.seq++
						if err := stream.track.WriteRTP(packet); err == nil {
							p.sent.Add(uint64(packet.MarshalSize()))
						}
					}
					stream.ts += 3000
				}
			}
		}()
	}
}

func processCPUSeconds() float64 {
	samples := []rmetrics.Sample{{Name: "/cpu/classes/total:cpu-seconds"}}
	rmetrics.Read(samples)
	if samples[0].Value.Kind() != rmetrics.KindFloat64 {
		return 0
	}
	return samples[0].Value.Float64()
}

func newLoadServer(t *testing.T) (*Server, string) {
	t.Helper()
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(validator.Close)
	mediaServer, err := New(config.Config{
		JWTSecret:       loadSecret(),
		ControlSecret:   "load-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		MaxPeersPerRoom: loadPeerCount,
		Simulcast:       true,
		SimulcastLayers: []config.SimulcastLayer{
			{RID: "q", Bitrate: 150_000}, {RID: "h", Bitrate: 400_000}, {RID: "f", Bitrate: 1_200_000},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	mediaServer.authHTTP = validator.Client()
	t.Cleanup(mediaServer.Close)
	httpServer := httptest.NewServer(mediaServer.Handler())
	t.Cleanup(httpServer.Close)
	return mediaServer, "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"
}

func TestLoad25Peers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	wsURL := strings.TrimSpace(os.Getenv("TARNMEDIA_LOAD_WS"))
	var server *Server
	if wsURL == "" {
		server, wsURL = newLoadServer(t)
	} else {
		t.Logf("using an external SFU at %s; metrics and process CPU come from that process", wsURL)
	}

	subscribers := make([]*loadPeer, 0, loadSubscribers)
	for index := 0; index < loadSubscribers; index++ {
		subscribers = append(subscribers, newLoadPeer(t, ctx, wsURL, fmt.Sprintf("sub-%02d", index), false))
	}
	defer func() {
		for _, subscriber := range subscribers {
			subscriber.close()
		}
	}()
	for index, subscriber := range subscribers {
		deadline := time.Now().Add(15 * time.Second)
		for subscriber.answers.Load() == 0 && time.Now().Before(deadline) {
			time.Sleep(20 * time.Millisecond)
		}
		if subscriber.answers.Load() == 0 {
			t.Fatalf("subscriber %d never answered the server offer", index)
		}
	}

	publisher := newLoadPeer(t, ctx, wsURL, "publisher", true)
	defer publisher.close()
	// Without an explicit state the SFU keeps the camera source inactive.
	if err := publisher.send("state", map[string]any{
		"micMuted": true, "micTransmitting": false, "cameraOn": true, "screenOn": false,
	}); err != nil {
		t.Fatal(err)
	}
	loadPeerPublisher := addSimulcastTracks(t, publisher.pc)
	offer, err := publisher.pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	midToSource := map[string]string{}
	for _, transceiver := range publisher.pc.GetTransceivers() {
		if transceiver.Mid() != "" {
			midToSource[transceiver.Mid()] = "camera"
		}
	}
	if err := publisher.send("publish_offer", publishOfferData{
		SDP: *publisher.pc.LocalDescription(), MidToSource: midToSource,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-publisher.publishAnswer:
	case <-time.After(15 * time.Second):
		t.Fatal("publisher never received publish_answer")
	}

	go loadPeerPublisher.runRTP(ctx)

	// All subscribers have to see media before the sample starts.
	deadline := time.Now().Add(40 * time.Second)
	for {
		ready := 0
		for _, subscriber := range subscribers {
			if subscriber.packets.Load() > 10 {
				ready++
			}
		}
		if ready == len(subscribers) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d subscribers received media", ready, len(subscribers))
		}
		time.Sleep(200 * time.Millisecond)
	}
	// The selector starts every subscriber on h and grows to f after a stable
	// eight-second window; give it that time before measuring.
	time.Sleep(10 * time.Second)

	startWall := time.Now()
	t.Logf("LOAD_SAMPLE_START_UNIX_MS=%d", startWall.UnixMilli())
	startCPU := processCPUSeconds()
	startSubBytes := make([]uint64, len(subscribers))
	for index, subscriber := range subscribers {
		startSubBytes[index] = subscriber.bytes.Load()
	}
	startSubPackets := make([]uint64, len(subscribers))
	for index, subscriber := range subscribers {
		startSubPackets[index] = subscriber.packets.Load()
	}
	startSent := loadPeerPublisher.sent.Load()
	var startForwarded, startPackets, startDrops uint64
	if server != nil {
		startForwarded = server.metrics.forwardedBytes.Load()
		startPackets = server.metrics.forwardedPackets.Load()
		startDrops = server.metrics.subscriberDrops.Load()
	}

	const sample = 10 * time.Second
	time.Sleep(sample)
	elapsed := time.Since(startWall).Seconds()
	t.Logf("LOAD_SAMPLE_END_UNIX_MS=%d", time.Now().UnixMilli())
	usedCPU := processCPUSeconds() - startCPU
	var forwards, forwardedPackets, drops uint64
	if server != nil {
		forwards = server.metrics.forwardedBytes.Load() - startForwarded
		forwardedPackets = server.metrics.forwardedPackets.Load() - startPackets
		drops = server.metrics.subscriberDrops.Load() - startDrops
	}
	sent := loadPeerPublisher.sent.Load() - startSent

	egress := uint64(0)
	receivedPackets := uint64(0)
	perSubscriber := make([]uint64, len(subscribers))
	for index, subscriber := range subscribers {
		bytes := subscriber.bytes.Load() - startSubBytes[index]
		perSubscriber[index] = bytes
		egress += bytes
		receivedPackets += subscriber.packets.Load() - startSubPackets[index]
	}
	sort.Slice(perSubscriber, func(i, j int) bool { return perSubscriber[i] < perSubscriber[j] })

	var layers map[string]int
	var waiting int
	var maxWait time.Duration
	if server != nil {
		layers, waiting, maxWait = server.simulcastState()
	}
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	t.Logf("peers=%d subscribers=%d sample=%.1fs", loadPeerCount, loadSubscribers, elapsed)
	t.Logf("ingress: %.2f Mbit/s (%.1f MB), egress: %.2f Mbit/s (%.1f MB)",
		float64(sent)*8/1e6/elapsed, float64(sent)/1e6,
		float64(egress)*8/1e6/elapsed, float64(egress)/1e6)
	if server != nil {
		t.Logf("forwarded: %.2f Mbit/s, packets=%d, subscriber queue drops=%d",
			float64(forwards)*8/1e6/elapsed, forwardedPackets, drops)
	}
	t.Logf("subscriber packets=%d, bytes min/median/max = %d/%d/%d",
		receivedPackets, perSubscriber[0], perSubscriber[len(perSubscriber)/2], perSubscriber[len(perSubscriber)-1])
	t.Logf("process CPU: %.2f CPU-seconds over %.1fs = %.2f cores (SFU + synthetic clients)",
		usedCPU, elapsed, usedCPU/elapsed)
	if server != nil {
		t.Logf("layers=%v waiting_keyframe=%d max_wait=%s heap=%d MB goroutines=%d",
			layers, waiting, maxWait, memStats.HeapAlloc/1e6, runtime.NumGoroutine())
	}

	if server != nil && drops > forwardedPackets/50 {
		t.Fatalf("queue drops = %d of %d forwarded packets, want below 2%%", drops, forwardedPackets)
	}
	if receivedPackets == 0 {
		t.Fatal("subscribers received no media")
	}
	if sent == 0 {
		t.Fatal("publisher sent no media")
	}
}
