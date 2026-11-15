package sfu

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

// Бюджет ICE-рестартов решает, переживёт ли разговор оборванный путь. Старый
// код засчитывал попытку ДО проверки, чем именно она закончилась: отложенный
// из-за идущей переговорировки рестарт (то есть нереальный) и рестарт на уже
// здоровом пути тратили бюджет наравне с настоящими. Две такие случайности
// закрывали пир, не попробовав перезапустить ICE ни разу.
//
// Тесты проверяют именно это: бюджет тратят только настоящие попытки.

const iceRecoveryRoom = "ice-room"

// iceRecoveryServer starts an SFU that accepts one room and returns it together
// with the websocket URL and the signing secret.
func iceRecoveryServer(t *testing.T) (*Server, string, string) {
	t.Helper()
	const jwtSecret = "ice-recovery-test-jwt-secret-long-enough"
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(validator.Close)

	mediaServer, err := New(config.Config{
		JWTSecret:       jwtSecret,
		ControlSecret:   "ice-recovery-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		UDPMin:          58310,
		UDPMax:          58400,
		MaxPeersPerRoom: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	mediaServer.authHTTP = validator.Client()
	t.Cleanup(mediaServer.Close)

	httpServer := httptest.NewServer(mediaServer.Handler())
	t.Cleanup(httpServer.Close)
	return mediaServer, "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws", jwtSecret
}

// silentPeer connects and stays silent: it never answers the offer, so the
// server-side peer keeps a pending local offer and its signaling state is not
// stable — exactly the situation in which an ICE restart cannot be applied.
type silentPeer struct {
	ws        *websocket.Conn
	stop      chan struct{}
	readError chan error
}

func dialSilentPeer(t *testing.T, ctx context.Context, wsURL, jwtSecret, id string) *silentPeer {
	t.Helper()
	header := http.Header{"Origin": []string{"https://tarnveil.ru"}}
	ws, _, err := websocket.DefaultDialer.DialContext(ctx, wsURL, header)
	if err != nil {
		t.Fatalf("dial signaling websocket %s: %v", id, err)
	}
	p := &silentPeer{ws: ws, stop: make(chan struct{}), readError: make(chan error, 1)}
	go p.read(ctx)
	if err := p.send("authenticate", map[string]string{"token": iceRecoveryToken(t, jwtSecret, id)}); err != nil {
		t.Fatalf("authenticate %s: %v", id, err)
	}
	return p
}

func (p *silentPeer) read(ctx context.Context) {
	for {
		_, _, err := p.ws.ReadMessage()
		if err != nil {
			select {
			case p.readError <- err:
			default:
			}
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

func (p *silentPeer) send(event string, data any) error {
	return p.ws.WriteJSON(map[string]any{"event": event, "data": data})
}

func (p *silentPeer) close() {
	close(p.stop)
	_ = p.ws.Close()
}

func iceRecoveryToken(t *testing.T, jwtSecret, id string) string {
	t.Helper()
	claims := auth.Claims{
		Room: iceRecoveryRoom, ParticipantID: id, UserID: "user-" + id,
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
		t.Fatalf("sign token for %s: %v", id, err)
	}
	return token
}

// waitServerSignalingState waits until the server-side peer reaches the wanted
// signaling state.
func waitServerSignalingState(t *testing.T, sp *peer, want webrtc.SignalingState) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if sp.pc.SignalingState() == want {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("server peer signaling state = %s, want %s", sp.pc.SignalingState(), want)
}

// waitServerPeer waits until the SFU registers the peer in the room. The join
// happens on the server's own goroutine, so the room may not exist yet.
func waitServerPeer(t *testing.T, s *Server, roomID, peerID string) *peer {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.RLock()
		currentRoom := s.rooms[roomID]
		s.mu.RUnlock()
		if currentRoom != nil {
			currentRoom.mu.Lock()
			current := currentRoom.peers[peerID]
			currentRoom.mu.Unlock()
			if current != nil {
				return current
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("peer %q never joined room %q", peerID, roomID)
	return nil
}

// recoveryCounters reads the recovery state of a peer.
func recoveryCounters(sp *peer) (restarts int, defers int) {
	sp.recoveryMu.Lock()
	defer sp.recoveryMu.Unlock()
	return sp.iceRestarts, sp.iceDefers
}

func resetRecoveryCounters(sp *peer) {
	sp.recoveryMu.Lock()
	sp.iceRestarts = 0
	sp.iceDefers = 0
	sp.recoveryMu.Unlock()
}

func TestICERecoveryPostponesWhileRenegotiationIsInFlight(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()

	mediaServer, wsURL, jwtSecret := iceRecoveryServer(t)
	silent := dialSilentPeer(t, ctx, wsURL, jwtSecret, "peer-silent")
	defer silent.close()

	sp := waitServerPeer(t, mediaServer, iceRecoveryRoom, "peer-silent")
	waitServerSignalingState(t, sp, webrtc.SignalingStateHaveLocalOffer)
	resetRecoveryCounters(sp)

	sp.recoverICE()
	sp.cancelICERecovery()

	restarts, defers := recoveryCounters(sp)
	if restarts != 0 {
		t.Fatalf("restart budget spent on a postponed recovery: iceRestarts=%d, want 0", restarts)
	}
	if defers != 1 {
		t.Fatalf("postponements = %d, want 1", defers)
	}
}

func TestICERecoveryDoesNotSpendBudgetOnHealthyPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()

	mediaServer, wsURL, jwtSecret := iceRecoveryServer(t)
	// This peer answers the offer and reaches a healthy transport.
	healthy := newLifecyclePeer(t, ctx, wsURL, jwtSecret, "peer-healthy", iceRecoveryRoom, true)
	defer healthy.close()
	waitLifecycleConnected(t, ctx, healthy)

	sp := waitServerPeer(t, mediaServer, iceRecoveryRoom, "peer-healthy")
	resetRecoveryCounters(sp)

	sp.recoverICE()
	sp.cancelICERecovery()

	restarts, defers := recoveryCounters(sp)
	if restarts != 0 {
		t.Fatalf("restart budget spent on a healthy path: iceRestarts=%d, want 0", restarts)
	}
	if defers != 0 {
		t.Fatalf("healthy path counted as a postponement: iceDefers=%d, want 0", defers)
	}
}

func TestICERecoveryGivesUpAfterTooManyPostponements(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), lifecycleSignalDeadline)
	defer cancel()

	mediaServer, wsURL, jwtSecret := iceRecoveryServer(t)
	silent := dialSilentPeer(t, ctx, wsURL, jwtSecret, "peer-stubborn")
	defer silent.close()

	sp := waitServerPeer(t, mediaServer, iceRecoveryRoom, "peer-stubborn")
	waitServerSignalingState(t, sp, webrtc.SignalingStateHaveLocalOffer)
	resetRecoveryCounters(sp)

	for i := 0; i < iceRecoveryDefers; i++ {
		sp.recoverICE()
		sp.cancelICERecovery()
	}
	_, defers := recoveryCounters(sp)
	if defers != iceRecoveryDefers {
		t.Fatalf("postponements = %d, want %d", defers, iceRecoveryDefers)
	}

	// The next postponement is the last one: the peer is closed instead of
	// waiting for a renegotiation that may never end.
	sp.recoverICE()
	select {
	case <-silent.readError:
	case <-time.After(5 * time.Second):
		t.Fatal("peer stayed open after repeated postponements")
	}
}
