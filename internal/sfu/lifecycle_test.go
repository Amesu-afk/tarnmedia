package sfu

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

const lifecycleSignalDeadline = 20 * time.Second

type lifecycleSignal struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data"`
}

type lifecyclePeer struct {
	id   string
	pc   *webrtc.PeerConnection
	ws   *websocket.Conn
	mu   sync.Mutex
	stop chan struct{}

	connected      chan struct{}
	connectedOnce  sync.Once
	readError      chan error
	participantNum chan int
	answeredOffers atomic.Uint64
	packetEvents   chan string
	packetCounts   sync.Map // participant ID -> *atomic.Uint64
	localTrack     *webrtc.TrackLocalStaticSample
	sampleOnce     sync.Once
}

func TestAbruptPeerLeaveAndReconnectKeepsRemainingRTP(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()

	const jwtSecret = "lifecycle-test-jwt-secret-long-enough"
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer validator.Close()

	mediaServer, err := New(config.Config{
		JWTSecret:       jwtSecret,
		ControlSecret:   "lifecycle-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		UDPMin:          58000,
		UDPMax:          58100,
		MaxPeersPerRoom: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	mediaServer.authHTTP = validator.Client()
	defer mediaServer.Close()

	httpServer := httptest.NewServer(mediaServer.Handler())
	defer httpServer.Close()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"

	// B publishes continuously; C subscribes to B and A. A is then abruptly
	// removed and reconnects under the same participant ID.
	b := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-b", "lifecycle-room", true)
	defer b.close()
	c := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-c", "lifecycle-room", true)
	defer c.close()

	waitLifecycleConnected(t, ctx, b)
	waitLifecycleConnected(t, ctx, c)
	waitParticipantCount(t, ctx, c, 2)
	waitPacketCount(t, ctx, c, "peer-b", 5)

	a := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-a", "lifecycle-room", true)
	defer a.close()
	waitLifecycleConnected(t, ctx, a)
	waitParticipantCount(t, ctx, c, 3)
	waitPacketCount(t, ctx, c, "peer-a", 5)

	// Establish that A's departure caused an updated offer to reach C, while
	// measuring B's live downlink before the abrupt socket close.
	answersBeforeLeave := c.answeredOffers.Load()
	bPacketsBeforeLeave := c.packetCount("peer-b")
	if err := a.ws.UnderlyingConn().Close(); err != nil {
		t.Fatalf("abruptly close peer A signaling socket: %v", err)
	}
	waitParticipantCount(t, ctx, c, 2)
	waitAnsweredOffers(t, ctx, c, answersBeforeLeave+1)
	waitPacketCount(t, ctx, c, "peer-b", bPacketsBeforeLeave+10)

	// Rejoining with the same participant ID must work after leave cleanup, and
	// both the old subscriber (C) and the publisher (B) must remain usable.
	answersBeforeRejoin := c.answeredOffers.Load()
	a2 := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-a", "lifecycle-room", true)
	defer a2.close()
	waitLifecycleConnected(t, ctx, a2)
	waitParticipantCount(t, ctx, c, 3)
	waitAnsweredOffers(t, ctx, c, answersBeforeRejoin+1)
	aPacketsBeforeRejoin := c.packetCount("peer-a")
	bPacketsBeforeRejoin := c.packetCount("peer-b")
	waitPacketCount(t, ctx, c, "peer-a", aPacketsBeforeRejoin+5)
	waitPacketCount(t, ctx, c, "peer-b", bPacketsBeforeRejoin+10)

	if state := waitPeerConnected(t, ctx, b); state != webrtc.PeerConnectionStateConnected {
		t.Fatalf("remaining publisher B state after A rejoined = %s, want connected", state)
	}
	if state := waitPeerConnected(t, ctx, c); state != webrtc.PeerConnectionStateConnected {
		t.Fatalf("remaining subscriber C state after A rejoined = %s, want connected", state)
	}
}

func TestICERestartKeepsTheCallAndRefreshesCredentials(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()

	const jwtSecret = "lifecycle-test-jwt-secret-long-enough"
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer validator.Close()

	mediaServer, err := New(config.Config{
		JWTSecret:       jwtSecret,
		ControlSecret:   "lifecycle-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		UDPMin:          58000,
		UDPMax:          58100,
		MaxPeersPerRoom: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	mediaServer.authHTTP = validator.Client()
	defer mediaServer.Close()

	httpServer := httptest.NewServer(mediaServer.Handler())
	defer httpServer.Close()
	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws"

	// B publishes and subscribes; A publishes so B has a live downlink to lose.
	b := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-b", "lifecycle-room", true)
	defer b.close()
	a := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-a", "lifecycle-room", true)
	defer a.close()

	waitLifecycleConnected(t, ctx, b)
	waitLifecycleConnected(t, ctx, a)
	waitParticipantCount(t, ctx, b, 2)
	waitPacketCount(t, ctx, b, "peer-a", 5)

	before := sdpAttribute(b.pc.RemoteDescription().SDP, "ice-ufrag")
	if before == "" {
		t.Fatal("negotiated SDP carries no ice-ufrag")
	}
	answers := b.answeredOffers.Load()
	packetsBeforeRestart := b.packetCount("peer-a")

	// This is what the SFU does on its own when a path stops passing media.
	if err := lookupServerPeer(t, mediaServer, "lifecycle-room", "peer-b").restartICE(); err != nil {
		t.Fatalf("restart ICE: %v", err)
	}
	waitAnsweredOffers(t, ctx, b, answers+1)

	after := sdpAttribute(b.pc.RemoteDescription().SDP, "ice-ufrag")
	if after == "" || after == before {
		t.Fatalf("ice-ufrag after the restart = %q, want a fresh one (was %q)", after, before)
	}
	// The restarted transport must keep carrying media instead of ending the call.
	waitPacketCount(t, ctx, b, "peer-a", packetsBeforeRestart+5)
	if state := waitPeerConnected(t, ctx, b); state != webrtc.PeerConnectionStateConnected {
		t.Fatalf("peer B state after ICE restart = %s, want connected", state)
	}
}

// lookupServerPeer reaches the SFU side of a signaling connection. Tests live in
// this package, so the room registry is available directly.
func lookupServerPeer(t *testing.T, s *Server, roomID, peerID string) *peer {
	t.Helper()
	s.mu.RLock()
	currentRoom := s.rooms[roomID]
	s.mu.RUnlock()
	if currentRoom == nil {
		t.Fatalf("room %q is not registered", roomID)
	}
	currentRoom.mu.Lock()
	defer currentRoom.mu.Unlock()
	current := currentRoom.peers[peerID]
	if current == nil {
		t.Fatalf("peer %q is not in room %q", peerID, roomID)
	}
	return current
}

// sdpAttribute returns the first value of an a=<name>: line. Every BUNDLE
// m-section repeats the same ICE credentials, so the first one is enough.
func sdpAttribute(sdp, name string) string {
	prefix := "a=" + name + ":"
	for _, line := range strings.Split(sdp, "\r\n") {
		if strings.HasPrefix(line, prefix) {
			return strings.TrimPrefix(line, prefix)
		}
	}
	return ""
}

func newLifecyclePeer(t *testing.T, ctx context.Context, wsURL, jwtSecret, id, room string, publish bool) *lifecyclePeer {
	t.Helper()
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create peer connection %s: %v", id, err)
	}
	p := &lifecyclePeer{
		id:             id,
		pc:             pc,
		stop:           make(chan struct{}),
		connected:      make(chan struct{}),
		readError:      make(chan error, 1),
		participantNum: make(chan int, 16),
		packetEvents:   make(chan string, 512),
	}
	if publish {
		p.localTrack, err = webrtc.NewTrackLocalStaticSample(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeOpus, ClockRate: 48_000, Channels: 2},
			"microphone", id,
		)
		if err != nil {
			_ = pc.Close()
			t.Fatalf("create Opus track %s: %v", id, err)
		}
		if _, err := pc.AddTrack(p.localTrack); err != nil {
			_ = pc.Close()
			t.Fatalf("add Opus track %s: %v", id, err)
		}
	}
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		if state == webrtc.PeerConnectionStateConnected {
			p.connectedOnce.Do(func() { close(p.connected) })
		}
	})
	pc.OnTrack(func(track *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		parts := strings.Split(track.ID(), ":")
		ownerID := "unknown"
		if len(parts) >= 3 {
			ownerID = parts[0]
		}
		counterValue, _ := p.packetCounts.LoadOrStore(ownerID, &atomic.Uint64{})
		counter := counterValue.(*atomic.Uint64)
		go func() {
			for {
				if _, _, readErr := track.ReadRTP(); readErr != nil {
					return
				}
				counter.Add(1)
				select {
				case p.packetEvents <- ownerID:
				default:
				}
			}
		}()
	})
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		p.send("candidate", candidate.ToJSON())
	})

	claims := auth.Claims{
		Room: room, ParticipantID: id, UserID: "user-" + id,
		SessionVersion: 1, Username: id, DisplayName: id,
		IssuedAtMS: time.Now().UnixMilli(),
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer: auth.DefaultIssuer, Subject: "user-" + id,
			Audience:  jwt.ClaimStrings{auth.Audience},
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	}
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(jwtSecret))
	if err != nil {
		_ = pc.Close()
		t.Fatalf("sign token for %s: %v", id, err)
	}
	header := http.Header{"Origin": []string{"https://tarnveil.ru"}}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		_ = pc.Close()
		t.Fatalf("dial signaling websocket %s: %v", id, err)
	}
	p.ws = ws
	go p.readSignals(ctx, publish)
	if err := p.send("authenticate", map[string]string{"token": token}); err != nil {
		p.close()
		t.Fatalf("authenticate %s: %v", id, err)
	}
	if publish {
		if err := p.send("state", map[string]any{"micMuted": false, "micTransmitting": true}); err != nil {
			p.close()
			t.Fatalf("enable microphone for %s: %v", id, err)
		}
	}
	return p
}

func (p *lifecyclePeer) readSignals(ctx context.Context, publish bool) {
	for {
		_, data, err := p.ws.ReadMessage()
		if err != nil {
			select {
			case p.readError <- err:
			default:
			}
			return
		}
		var message lifecycleSignal
		if err := json.Unmarshal(data, &message); err != nil {
			select {
			case p.readError <- fmt.Errorf("decode signaling message: %w", err):
			default:
			}
			return
		}
		switch message.Event {
		case "participants":
			var participants []json.RawMessage
			var payload struct {
				Participants []json.RawMessage `json:"participants"`
			}
			if err := json.Unmarshal(message.Data, &payload); err == nil {
				participants = payload.Participants
			}
			select {
			case p.participantNum <- len(participants):
			default:
			}
		case "offer":
			var offer webrtc.SessionDescription
			if err := json.Unmarshal(message.Data, &offer); err != nil {
				p.fail(fmt.Errorf("decode offer: %w", err))
				return
			}
			if err := p.pc.SetRemoteDescription(offer); err != nil {
				p.fail(fmt.Errorf("set remote offer: %w", err))
				return
			}
			if err := p.prepareAnswer(publish); err != nil {
				p.fail(err)
				return
			}
			answer, err := p.pc.CreateAnswer(nil)
			if err != nil {
				p.fail(fmt.Errorf("create answer: %w", err))
				return
			}
			if err := p.pc.SetLocalDescription(answer); err != nil {
				p.fail(fmt.Errorf("set local answer: %w", err))
				return
			}
			if err := p.send("answer", p.pc.LocalDescription()); err != nil {
				p.fail(fmt.Errorf("send answer: %w", err))
				return
			}
			p.answeredOffers.Add(1)
		case "candidate":
			var candidate webrtc.ICECandidateInit
			if err := json.Unmarshal(message.Data, &candidate); err != nil {
				p.fail(fmt.Errorf("decode candidate: %w", err))
				return
			}
			if err := p.pc.AddICECandidate(candidate); err != nil {
				p.fail(fmt.Errorf("add ICE candidate: %w", err))
				return
			}
		case "error":
			p.fail(fmt.Errorf("SFU signaling error: %s", message.Data))
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		default:
		}
	}
}

func (p *lifecyclePeer) prepareAnswer(publish bool) error {
	transceivers := p.pc.GetTransceivers()
	// The SFU's first offer uses microphone, camera, screen, screen-audio m-lines.
	// Attach to its first audio sender, matching the production client's slot order.
	var microphone *webrtc.RTPTransceiver
	if len(transceivers) > 0 {
		microphone = transceivers[0]
	}
	if publish && p.localTrack != nil && microphone != nil {
		p.sampleOnce.Do(func() { go p.writeSamples() })
	}
	return nil
}

func (p *lifecyclePeer) writeSamples() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	sample := media.Sample{Data: []byte{0xF8, 0xFF, 0xFE}, Duration: 20 * time.Millisecond}
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			if err := p.localTrack.WriteSample(sample); err != nil {
				return
			}
		}
	}
}

func (p *lifecyclePeer) send(event string, data any) error {
	encoded, err := json.Marshal(map[string]any{"event": event, "data": data})
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ws == nil {
		return fmt.Errorf("websocket is not ready")
	}
	return p.ws.WriteMessage(websocket.TextMessage, encoded)
}

func (p *lifecyclePeer) fail(err error) {
	select {
	case p.readError <- err:
	default:
	}
}

func (p *lifecyclePeer) packetCount(ownerID string) uint64 {
	value, ok := p.packetCounts.Load(ownerID)
	if !ok {
		return 0
	}
	return value.(*atomic.Uint64).Load()
}

func (p *lifecyclePeer) close() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
	if p.ws != nil {
		_ = p.ws.Close()
	}
	if p.pc != nil {
		_ = p.pc.Close()
	}
}

func waitLifecycleConnected(t *testing.T, ctx context.Context, p *lifecyclePeer) {
	t.Helper()
	select {
	case <-p.connected:
	case err := <-p.readError:
		t.Fatalf("peer %s signaling failed before connection: %v", p.id, err)
	case <-ctx.Done():
		t.Fatalf("peer %s did not connect before deadline: state=%s", p.id, p.pc.ConnectionState())
	}
}

// waitPeerConnected polls the transport state. After an ICE restart Pion can
// report "connecting" for a moment even though packets already flow, so a
// single immediate check made the test fail depending on scheduling.
func waitPeerConnected(t *testing.T, ctx context.Context, p *lifecyclePeer) webrtc.PeerConnectionState {
	t.Helper()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		state := p.pc.ConnectionState()
		if state == webrtc.PeerConnectionStateConnected || state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			return state
		}
		select {
		case <-ctx.Done():
			return state
		case <-ticker.C:
		}
	}
}

func waitParticipantCount(t *testing.T, ctx context.Context, p *lifecyclePeer, want int) {
	t.Helper()
	for {
		select {
		case got := <-p.participantNum:
			if got == want {
				return
			}
		case err := <-p.readError:
			t.Fatalf("peer %s signaling failed while waiting for %d participants: %v", p.id, want, err)
		case <-ctx.Done():
			t.Fatalf("peer %s did not receive participant count %d before deadline", p.id, want)
		}
	}
}

func waitAnsweredOffers(t *testing.T, ctx context.Context, p *lifecyclePeer, want uint64) {
	t.Helper()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for p.answeredOffers.Load() < want {
		select {
		case err := <-p.readError:
			t.Fatalf("peer %s signaling failed while waiting for renegotiation: %v", p.id, err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("peer %s answered %d offers, want at least %d", p.id, p.answeredOffers.Load(), want)
		}
	}
}

func waitPacketCount(t *testing.T, ctx context.Context, p *lifecyclePeer, ownerID string, want uint64) {
	t.Helper()
	for p.packetCount(ownerID) < want {
		select {
		case <-p.packetEvents:
		case err := <-p.readError:
			t.Fatalf("peer %s signaling failed while waiting for RTP from %s: %v", p.id, ownerID, err)
		case <-ctx.Done():
			t.Fatalf("peer %s received %d RTP packets from %s, want at least %d", p.id, p.packetCount(ownerID), ownerID, want)
		}
	}
}
