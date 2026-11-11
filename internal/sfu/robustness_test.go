package sfu

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

// sync() used to keep the room lock while writing to signaling sockets, so one
// stuck subscriber froze the whole room: new uplinks, departures and camera
// toggles waited for the slowest client. The lock may only cover the snapshot.
func TestRoomSyncDoesNotHoldTheRoomLock(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatalf("create PeerConnection: %v", err)
	}
	defer func() { _ = pc.Close() }()

	metrics := &serverMetrics{}
	currentRoom := &room{
		id: "lock-room", maxPeers: 2, metrics: metrics,
		peers:   make(map[string]*peer),
		uplinks: make(map[string]localTrack),
		tracks:  make(map[string]localTrack),
	}
	// offered=true and no tracks means the peer is up to date, so syncPeer
	// returns without touching the (absent) signaling socket.
	current := &peer{id: "p", pc: pc, room: currentRoom, offered: true}
	currentRoom.peers[current.id] = current

	// Simulate a negotiation that is writing its offer right now.
	current.signalMu.Lock()
	done := make(chan struct{})
	go func() {
		currentRoom.sync()
		close(done)
	}()

	// Give the goroutine time to reach the point where it waits for the peer.
	// A slower scheduler can only make this check pass, never fail it: the old
	// implementation takes the room lock in its very first statement.
	time.Sleep(100 * time.Millisecond)
	if !currentRoom.mu.TryLock() {
		current.signalMu.Unlock()
		<-done
		t.Fatal("room lock is held while a renegotiation waits for the peer")
	}
	currentRoom.mu.Unlock()

	current.signalMu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sync did not finish after the peer released its signaling lock")
	}
}

// A client repeats its state after every reconnect. Broadcasting the whole
// participant list when nothing changed burns the signaling budget for no
// reason, and a real change still has to reach everybody.
func TestDuplicateMediaStateDoesNotRebroadcastParticipants(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()
	_, wsURL, jwtSecret := newRobustnessServer(t)

	a := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-a", false)
	defer a.close()
	b := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-b", false)
	defer b.close()
	waitLifecycleConnected(t, ctx, a)
	waitLifecycleConnected(t, ctx, b)
	waitParticipantCount(t, ctx, a, 2)
	// Даём последнему снапшоту о входе дойти, чтобы он не сошёл за реакцию на
	// первое сообщение о состоянии.
	time.Sleep(50 * time.Millisecond)
	drainParticipantSnapshots(a)

	// Peer joins fail-closed as muted, so going unmuted is a real change.
	unmuted := map[string]any{"micMuted": false, "micTransmitting": true}
	if err := b.send("state", unmuted); err != nil {
		t.Fatalf("send state: %v", err)
	}
	waitParticipantSnapshot(t, ctx, a, 2*time.Second)
	drainParticipantSnapshots(a)

	// Exactly the same state must not produce another snapshot.
	if err := b.send("state", unmuted); err != nil {
		t.Fatalf("send duplicate state: %v", err)
	}
	expectNoParticipantSnapshot(t, ctx, a, 400*time.Millisecond)

	// A real change still has to be broadcast.
	if err := b.send("state", map[string]any{"micMuted": false, "micTransmitting": true, "cameraOn": true}); err != nil {
		t.Fatalf("send changed state: %v", err)
	}
	waitParticipantSnapshot(t, ctx, a, 2*time.Second)
}

func newRobustnessServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	const jwtSecret = "lifecycle-test-jwt-secret-long-enough"
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(validator.Close)

	mediaServer, err := New(config.Config{
		JWTSecret:       jwtSecret,
		ControlSecret:   "lifecycle-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		UDPMin:          58200,
		UDPMax:          58300,
		MaxPeersPerRoom: 10,
	})
	if err != nil {
		t.Fatalf("initialize TarnMedia: %v", err)
	}
	mediaServer.authHTTP = validator.Client()
	t.Cleanup(mediaServer.Close)

	httpServer := httptest.NewServer(mediaServer.Handler())
	t.Cleanup(httpServer.Close)
	return mediaServer, "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws", jwtSecret
}

func drainParticipantSnapshots(p *lifecyclePeer) {
	for {
		select {
		case <-p.participantNum:
		default:
			return
		}
	}
}

func waitParticipantSnapshot(t *testing.T, ctx context.Context, p *lifecyclePeer, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-p.participantNum:
			return
		case err := <-p.readError:
			t.Fatalf("peer %s signaling failed: %v", p.id, err)
		case <-ctx.Done():
			t.Fatal("context expired while waiting for a participant snapshot")
		case <-deadline:
			t.Fatal("no participant snapshot arrived after a real state change")
		}
	}
}

func expectNoParticipantSnapshot(t *testing.T, ctx context.Context, p *lifecyclePeer, window time.Duration) {
	t.Helper()
	deadline := time.After(window)
	for {
		select {
		case got := <-p.participantNum:
			t.Fatalf("participant snapshot (count %d) was rebroadcast for an unchanged state", got)
		case err := <-p.readError:
			t.Fatalf("peer %s signaling failed: %v", p.id, err)
		case <-ctx.Done():
			t.Fatal("context expired while waiting for quiet signaling")
		case <-deadline:
			return
		}
	}
}

// A briefly unavailable API must not turn into a failed join, while a real
// refusal has to fail closed at once.
func TestValidateSessionRetriesTransientFailure(t *testing.T) {
	var calls atomic.Int32
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer validator.Close()

	mediaServer := newValidationServer(t, validator.URL)
	mediaServer.authHTTP = validator.Client()
	if err := mediaServer.validateSession(auth.Claims{UserID: "user-1", SessionVersion: 1}); err != nil {
		t.Fatalf("validateSession after a transient failure: %v", err)
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("validation requests = %d, want 2", got)
	}
}

func TestValidateSessionFailsClosedOnRefusal(t *testing.T) {
	var calls atomic.Int32
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusForbidden)
	}))
	defer validator.Close()

	mediaServer := newValidationServer(t, validator.URL)
	mediaServer.authHTTP = validator.Client()
	err := mediaServer.validateSession(auth.Claims{UserID: "user-1", SessionVersion: 1})
	if !errors.Is(err, errSessionInactive) {
		t.Fatalf("validateSession error = %v, want errSessionInactive", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("validation requests = %d, want a single one", got)
	}
}

// A publisher that stops sending RTP is the first sign of one-way audio. The
// watchdog must notice it once, notice the recovery, and leave muted
// publishers alone — they send nothing by design.
func TestStallWatchdogFlagsSilentPublisher(t *testing.T) {
	mediaServer := &Server{rooms: make(map[string]*room)}
	currentRoom := &room{
		id: "room-1", maxPeers: 4, metrics: &serverMetrics{},
		peers:   make(map[string]*peer),
		uplinks: make(map[string]localTrack),
		tracks:  make(map[string]localTrack),
	}
	owner := &peer{id: "pub"}
	owner.state = mediaState{MicMuted: false, MicTransmitting: true}
	activity := &trackActivity{}
	currentRoom.uplinks["pub:microphone:1"] = localTrack{
		ownerID: owner.id, source: "microphone", owner: owner, activity: activity,
	}
	mediaServer.rooms[currentRoom.id] = currentRoom

	activity.lastPacket.Store(time.Now().Add(-2 * mediaStallTimeout).UnixNano())
	mediaServer.checkMediaStalls(time.Now())
	if !activity.stalled.Load() {
		t.Fatal("silent publisher was not flagged")
	}

	activity.lastPacket.Store(time.Now().UnixNano())
	mediaServer.checkMediaStalls(time.Now())
	if activity.stalled.Load() {
		t.Fatal("resumed publisher stayed flagged")
	}

	owner.stateMu.Lock()
	owner.state = mediaState{MicMuted: true}
	owner.stateMu.Unlock()
	activity.lastPacket.Store(time.Now().Add(-2 * mediaStallTimeout).UnixNano())
	mediaServer.checkMediaStalls(time.Now())
	if activity.stalled.Load() {
		t.Fatal("muted publisher was reported as stalled")
	}
}

func newValidationServer(t *testing.T, authURL string) *Server {
	t.Helper()
	mediaServer, err := New(config.Config{
		JWTSecret:       strings.Repeat("j", 40),
		ControlSecret:   strings.Repeat("c", 40),
		Issuer:          auth.DefaultIssuer,
		AuthURL:         authURL,
		AllowedOrigins:  map[string]struct{}{},
		UDPMin:          58400,
		UDPMax:          58500,
		MaxPeersPerRoom: 2,
	})
	if err != nil {
		t.Fatalf("initialize TarnMedia: %v", err)
	}
	t.Cleanup(mediaServer.Close)
	return mediaServer
}
