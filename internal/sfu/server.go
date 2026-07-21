package sfu

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

const (
	maxSignalMessageBytes  = 1 << 20
	maxControlMessageBytes = 8 << 10
	signalRateWindow       = 10 * time.Second
	maxSignalsPerWindow    = 240
	websocketPongWait      = 45 * time.Second
	websocketPingPeriod    = 15 * time.Second
	revocationRetention    = 10 * time.Minute
)

type serverMetrics struct {
	websocketConnections atomic.Uint64
	authFailures         atomic.Uint64
	rejectedConnections  atomic.Uint64
	signalErrors         atomic.Uint64
	signalRateLimited    atomic.Uint64
	forwardedPackets     atomic.Uint64
	forwardedBytes       atomic.Uint64
	controlRequests      atomic.Uint64
	controlAuthFailures  atomic.Uint64
	evictedPeers         atomic.Uint64
}

type revocation struct {
	revokedAt               time.Time
	expiresAt               time.Time
	minActiveSessionVersion int
	hasActiveSessionVersion bool
}

type Server struct {
	cfg      config.Config
	api      *webrtc.API
	pcConfig webrtc.Configuration
	authHTTP *http.Client
	upgrader websocket.Upgrader

	mu          sync.RWMutex
	rooms       map[string]*room
	revocations map[string]revocation
	metrics     serverMetrics
	ready       atomic.Bool
}

type signalMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type authenticateData struct {
	Token string `json:"token"`
}

type mediaState struct {
	MicMuted bool `json:"micMuted"`
	CameraOn bool `json:"cameraOn"`
	ScreenOn bool `json:"screenOn"`
}

type participantView struct {
	ParticipantID string `json:"participantId"`
	UserID        string `json:"userId"`
	Username      string `json:"username"`
	DisplayName   string `json:"displayName"`
	AvatarURL     string `json:"avatarUrl,omitempty"`
	MicMuted      bool   `json:"micMuted"`
	CameraOn      bool   `json:"cameraOn"`
	ScreenOn      bool   `json:"screenOn"`
}

type trackView struct {
	MID           string `json:"mid"`
	TrackID       string `json:"trackId"`
	ParticipantID string `json:"participantId"`
	Source        string `json:"source"`
}

type localTrack struct {
	ownerID string
	source  string
	track   *webrtc.TrackLocalStaticRTP
}

type room struct {
	id       string
	maxPeers int
	mu       sync.Mutex
	peers    map[string]*peer
	uplinks  map[string]localTrack
	tracks   map[string]localTrack
	metrics  *serverMetrics
}

type peer struct {
	id             string
	claims         auth.Claims
	room           *room
	pc             *webrtc.PeerConnection
	ws             *websocket.Conn
	receiveSources map[*webrtc.RTPReceiver]string

	writeMu                 sync.Mutex
	signalMu                sync.Mutex
	stateMu                 sync.RWMutex
	state                   mediaState
	closed                  sync.Once
	offered                 bool
	pendingRemoteCandidates []webrtc.ICECandidateInit
	rateMu                  sync.Mutex
	rateWindowStarted       time.Time
	rateWindowCount         int
}

type controlCommand struct {
	Action          string `json:"action"`
	Room            string `json:"room"`
	UserID          string `json:"userId,omitempty"`
	RevokedBeforeMS int64  `json:"revokedBeforeMs"`
	SessionVersion  *int   `json:"sessionVersion,omitempty"`
}

func New(cfg config.Config) (*Server, error) {
	mediaEngine := &webrtc.MediaEngine{}
	if err := mediaEngine.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("register WebRTC codecs: %w", err)
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptors); err != nil {
		return nil, fmt.Errorf("register WebRTC interceptors: %w", err)
	}
	settings := webrtc.SettingEngine{}
	if err := settings.SetEphemeralUDPPortRange(cfg.UDPMin, cfg.UDPMax); err != nil {
		return nil, fmt.Errorf("configure UDP port range: %w", err)
	}
	// The production media endpoint is IPv4-only, so gather only the UDP
	// candidates that can be advertised through TARNMEDIA_PUBLIC_IP.
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	if cfg.PublicIP != "" {
		settings.SetNAT1To1IPs([]string{cfg.PublicIP}, webrtc.ICECandidateTypeHost)
	}

	pcConfig := webrtc.Configuration{}
	if len(cfg.ICEURLs) > 0 {
		pcConfig.ICEServers = []webrtc.ICEServer{{
			URLs:       cfg.ICEURLs,
			Username:   cfg.ICEUsername,
			Credential: cfg.ICECredential,
		}}
	}

	server := &Server{
		cfg: cfg,
		api: webrtc.NewAPI(
			webrtc.WithMediaEngine(mediaEngine),
			webrtc.WithInterceptorRegistry(interceptors),
			webrtc.WithSettingEngine(settings),
		),
		pcConfig:    pcConfig,
		authHTTP:    &http.Client{Timeout: 2 * time.Second},
		rooms:       make(map[string]*room),
		revocations: make(map[string]revocation),
	}
	server.upgrader = websocket.Upgrader{
		HandshakeTimeout: 8 * time.Second,
		CheckOrigin: func(r *http.Request) bool {
			return cfg.OriginAllowed(r.Header.Get("Origin"))
		},
	}
	return server, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /v1/ws", s.handleWebSocket)
	return mux
}

func (s *Server) ControlHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /ready", s.handleReady)
	mux.HandleFunc("GET /metrics", s.handleMetrics)
	mux.HandleFunc("POST /v1/control", s.handleControl)
	return mux
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) RunMaintenance(stop <-chan struct{}) {
	keyFrameTicker := time.NewTicker(3 * time.Second)
	pingTicker := time.NewTicker(websocketPingPeriod)
	cleanupTicker := time.NewTicker(time.Minute)
	defer keyFrameTicker.Stop()
	defer pingTicker.Stop()
	defer cleanupTicker.Stop()
	for {
		select {
		case <-keyFrameTicker.C:
			s.dispatchKeyFrames()
		case <-pingTicker.C:
			s.pingPeers()
		case <-cleanupTicker.C:
			s.cleanupRevocations()
		case <-stop:
			return
		}
	}
}

func (s *Server) Close() {
	s.ready.Store(false)
	s.mu.Lock()
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.rooms = make(map[string]*room)
	s.mu.Unlock()
	for _, current := range rooms {
		current.close()
	}
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.RLock()
	roomCount := len(s.rooms)
	peerCount := 0
	for _, current := range s.rooms {
		current.mu.Lock()
		peerCount += len(current.peers)
		current.mu.Unlock()
	}
	s.mu.RUnlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"status": "ok", "service": "tarnmedia", "rooms": roomCount, "peers": peerCount,
	})
}

func (s *Server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not_ready", "service": "tarnmedia"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "tarnmedia"})
}

func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	rooms, peers := s.roomAndPeerCount()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = fmt.Fprintf(w, "# TYPE tarnmedia_rooms gauge\ntarnmedia_rooms %d\n", rooms)
	_, _ = fmt.Fprintf(w, "# TYPE tarnmedia_peers gauge\ntarnmedia_peers %d\n", peers)
	_, _ = fmt.Fprintf(w, "# TYPE tarnmedia_ready gauge\ntarnmedia_ready %d\n", boolMetric(s.ready.Load()))
	writeCounter(w, "tarnmedia_websocket_connections_total", s.metrics.websocketConnections.Load())
	writeCounter(w, "tarnmedia_auth_failures_total", s.metrics.authFailures.Load())
	writeCounter(w, "tarnmedia_rejected_connections_total", s.metrics.rejectedConnections.Load())
	writeCounter(w, "tarnmedia_signal_errors_total", s.metrics.signalErrors.Load())
	writeCounter(w, "tarnmedia_signal_rate_limited_total", s.metrics.signalRateLimited.Load())
	writeCounter(w, "tarnmedia_forwarded_packets_total", s.metrics.forwardedPackets.Load())
	writeCounter(w, "tarnmedia_forwarded_bytes_total", s.metrics.forwardedBytes.Load())
	writeCounter(w, "tarnmedia_control_requests_total", s.metrics.controlRequests.Load())
	writeCounter(w, "tarnmedia_control_auth_failures_total", s.metrics.controlAuthFailures.Load())
	writeCounter(w, "tarnmedia_evicted_peers_total", s.metrics.evictedPeers.Load())
}

func (s *Server) handleControl(w http.ResponseWriter, r *http.Request) {
	s.metrics.controlRequests.Add(1)
	if !s.controlAuthorized(r.Header.Get("Authorization")) {
		s.metrics.controlAuthFailures.Add(1)
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxControlMessageBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	command := controlCommand{}
	if err := decoder.Decode(&command); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid control command"})
		return
	}

	var affected int
	revokedAt := time.UnixMilli(command.RevokedBeforeMS)
	if command.RevokedBeforeMS <= 0 || revokedAt.Before(time.Now().Add(-time.Hour)) || revokedAt.After(time.Now().Add(5*time.Minute)) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid revocation time"})
		return
	}
	switch command.Action {
	case "closeRoom":
		if !validControlIdentifier(command.Room, 180) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid room"})
			return
		}
		affected = s.closeRoom(command.Room, revokedAt)
	case "evictUser":
		if !validControlIdentifier(command.Room, 180) || !validControlIdentifier(command.UserID, 180) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid user"})
			return
		}
		affected = s.evictUser(command.Room, command.UserID, revokedAt)
	case "revokeUser":
		if !validControlIdentifier(command.UserID, 180) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid user"})
			return
		}
		if command.SessionVersion != nil && *command.SessionVersion < 0 {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid session version"})
			return
		}
		affected = s.revokeUser(command.UserID, revokedAt, command.SessionVersion)
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "unsupported action"})
		return
	}
	s.metrics.evictedPeers.Add(uint64(affected))
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "affectedPeers": affected})
}

// revokeUser applies to every room. Global password changes and account bans
// cannot reliably enumerate a user's current DM/call rooms at the API layer.
func (s *Server) revokeUser(userID string, revokedAt time.Time, activeSessionVersion *int) int {
	now := time.Now()
	item := revocation{revokedAt: revokedAt, expiresAt: now.Add(revocationRetention)}
	if activeSessionVersion != nil {
		item.minActiveSessionVersion = *activeSessionVersion
		item.hasActiveSessionVersion = true
	}
	s.mu.Lock()
	s.revocations[revocationKey("", userID)] = item
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.mu.Unlock()

	affected := 0
	for _, current := range rooms {
		removed, empty := current.removeUser(userID)
		affected += removed
		if empty {
			s.mu.Lock()
			if s.rooms[current.id] == current {
				delete(s.rooms, current.id)
			}
			s.mu.Unlock()
		}
	}
	return affected
}

func (s *Server) controlAuthorized(header string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(header, prefix) {
		return false
	}
	provided := []byte(strings.TrimSpace(strings.TrimPrefix(header, prefix)))
	expected := []byte(s.cfg.ControlSecret)
	return len(provided) == len(expected) && subtle.ConstantTimeCompare(provided, expected) == 1
}

func (s *Server) roomAndPeerCount() (int, int) {
	s.mu.RLock()
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.mu.RUnlock()
	peers := 0
	for _, current := range rooms {
		current.mu.Lock()
		peers += len(current.peers)
		current.mu.Unlock()
	}
	return len(rooms), peers
}

func (s *Server) closeRoom(roomID string, revokedAt time.Time) int {
	now := time.Now()
	s.mu.Lock()
	s.revocations[revocationKey(roomID, "")] = revocation{revokedAt: revokedAt, expiresAt: now.Add(revocationRetention)}
	current := s.rooms[roomID]
	delete(s.rooms, roomID)
	s.mu.Unlock()
	if current == nil {
		return 0
	}
	current.mu.Lock()
	peerCount := len(current.peers)
	current.mu.Unlock()
	current.close()
	return peerCount
}

func (s *Server) evictUser(roomID, userID string, revokedAt time.Time) int {
	now := time.Now()
	s.mu.Lock()
	s.revocations[revocationKey(roomID, userID)] = revocation{revokedAt: revokedAt, expiresAt: now.Add(revocationRetention)}
	current := s.rooms[roomID]
	s.mu.Unlock()
	if current == nil {
		return 0
	}
	affected, empty := current.removeUser(userID)
	if empty {
		s.mu.Lock()
		if s.rooms[roomID] == current {
			delete(s.rooms, roomID)
		}
		s.mu.Unlock()
	}
	return affected
}

func (s *Server) tokenRevoked(claims auth.Claims) bool {
	issuedAt := time.UnixMilli(claims.IssuedAtMS)
	s.mu.RLock()
	roomRevocation, roomRevoked := s.revocations[revocationKey(claims.Room, "")]
	userRevocation, userRevoked := s.revocations[revocationKey(claims.Room, claims.UserID)]
	globalRevocation, globallyRevoked := s.revocations[revocationKey("", claims.UserID)]
	s.mu.RUnlock()
	globalInvalid := globallyRevoked && ((!globalRevocation.hasActiveSessionVersion && !issuedAt.After(globalRevocation.revokedAt)) ||
		(globalRevocation.hasActiveSessionVersion && claims.SessionVersion < globalRevocation.minActiveSessionVersion))
	return (roomRevoked && !issuedAt.After(roomRevocation.revokedAt)) ||
		(userRevoked && !issuedAt.After(userRevocation.revokedAt)) ||
		globalInvalid
}

func (s *Server) cleanupRevocations() {
	now := time.Now()
	s.mu.Lock()
	for key, item := range s.revocations {
		if now.After(item.expiresAt) {
			delete(s.revocations, key)
		}
	}
	s.mu.Unlock()
}

func (s *Server) pingPeers() {
	s.mu.RLock()
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.mu.RUnlock()
	for _, current := range rooms {
		current.mu.Lock()
		peers := make([]*peer, 0, len(current.peers))
		for _, currentPeer := range current.peers {
			peers = append(peers, currentPeer)
		}
		current.mu.Unlock()
		for _, currentPeer := range peers {
			if err := currentPeer.ping(); err != nil {
				_ = currentPeer.ws.Close()
			}
		}
	}
}

func revocationKey(roomID, userID string) string {
	return roomID + "\x00" + userID
}

func validControlIdentifier(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, current := range value {
		if current < 0x20 || current == 0x7f {
			return false
		}
	}
	return true
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	s.metrics.websocketConnections.Add(1)
	conn.SetReadLimit(maxSignalMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))

	claims, err := s.authenticate(conn)
	if err != nil {
		s.metrics.authFailures.Add(1)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	currentPeer, err := s.join(claims, conn)
	if err != nil {
		s.metrics.rejectedConnections.Add(1)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	defer s.leave(claims.Room, currentPeer.id)
	_ = conn.SetReadDeadline(time.Now().Add(websocketPongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(websocketPongWait))
	})

	_ = currentPeer.write("authenticated", map[string]any{
		"room":            claims.Room,
		"participantId":   claims.ParticipantID,
		"userId":          claims.UserID,
		"protocolVersion": 1,
	})
	currentPeer.room.broadcastParticipants()
	currentPeer.room.sync()

	for {
		message := signalMessage{}
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		if !currentPeer.allowSignal() {
			s.metrics.signalRateLimited.Add(1)
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "signaling rate limit exceeded"), time.Now().Add(time.Second))
			return
		}
		if err := currentPeer.handleSignal(message); err != nil {
			s.metrics.signalErrors.Add(1)
			slog.Warn("invalid signaling message", "room", claims.Room, "peer", claims.ParticipantID, "error", err)
			_ = currentPeer.write("error", map[string]string{"message": err.Error()})
		}
	}
}

func (s *Server) authenticate(conn *websocket.Conn) (auth.Claims, error) {
	message := signalMessage{}
	if err := conn.ReadJSON(&message); err != nil {
		return auth.Claims{}, errors.New("authentication message required")
	}
	if message.Event != "authenticate" {
		return auth.Claims{}, errors.New("first message must be authenticate")
	}
	data := authenticateData{}
	if err := json.Unmarshal(message.Data, &data); err != nil || data.Token == "" {
		return auth.Claims{}, errors.New("media token required")
	}
	claims, err := auth.Parse(data.Token, s.cfg.JWTSecret)
	if err != nil {
		return auth.Claims{}, err
	}
	if err := s.validateSession(claims); err != nil {
		return auth.Claims{}, err
	}
	return claims, nil
}

// validateSession keeps the SFU stateless with respect to user accounts while
// still making a JWT revocable after a password change or ban. The endpoint
// is loopback-only in normal deployment and requires the control secret.
func (s *Server) validateSession(claims auth.Claims) error {
	body, err := json.Marshal(struct {
		UserID         string `json:"userId"`
		SessionVersion int    `json:"sessionVersion"`
	}{UserID: claims.UserID, SessionVersion: claims.SessionVersion})
	if err != nil {
		return errors.New("media session validation failed")
	}
	req, err := http.NewRequest(http.MethodPost, s.cfg.AuthURL, bytes.NewReader(body))
	if err != nil {
		return errors.New("media session validation is unavailable")
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.ControlSecret)
	req.Header.Set("Content-Type", "application/json")
	response, err := s.authHTTP.Do(req)
	if err != nil {
		return errors.New("media session validation is unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		return errors.New("media session is no longer active")
	}
	return nil
}

func (s *Server) join(claims auth.Claims, conn *websocket.Conn) (*peer, error) {
	if s.tokenRevoked(claims) {
		return nil, errors.New("media token was revoked")
	}
	pc, err := s.api.NewPeerConnection(s.pcConfig)
	if err != nil {
		return nil, fmt.Errorf("create PeerConnection: %w", err)
	}
	currentPeer := &peer{
		id: claims.ParticipantID, claims: claims, pc: pc, ws: conn,
		receiveSources: make(map[*webrtc.RTPReceiver]string),
		state:          mediaState{MicMuted: true},
	}

	for _, input := range []struct {
		kind   webrtc.RTPCodecType
		source string
	}{
		{kind: webrtc.RTPCodecTypeAudio, source: "microphone"},
		{kind: webrtc.RTPCodecTypeVideo, source: "camera"},
		{kind: webrtc.RTPCodecTypeVideo, source: "screen"},
	} {
		transceiver, err := pc.AddTransceiverFromKind(input.kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
		if err != nil {
			_ = pc.Close()
			return nil, fmt.Errorf("add receiving transceiver: %w", err)
		}
		currentPeer.receiveSources[transceiver.Receiver()] = input.source
	}

	s.mu.Lock()
	currentRoom := s.rooms[claims.Room]
	if currentRoom == nil {
		currentRoom = &room{
			id: claims.Room, maxPeers: s.cfg.MaxPeersPerRoom,
			peers:   make(map[string]*peer),
			uplinks: make(map[string]localTrack), tracks: make(map[string]localTrack),
			metrics: &s.metrics,
		}
		s.rooms[claims.Room] = currentRoom
	}
	s.mu.Unlock()

	currentRoom.mu.Lock()
	if len(currentRoom.peers) >= currentRoom.maxPeers {
		currentRoom.mu.Unlock()
		_ = pc.Close()
		return nil, errors.New("room participant limit reached")
	}
	if _, exists := currentRoom.peers[currentPeer.id]; exists {
		currentRoom.mu.Unlock()
		_ = pc.Close()
		return nil, errors.New("participant is already connected")
	}
	currentPeer.room = currentRoom
	currentRoom.peers[currentPeer.id] = currentPeer
	currentRoom.mu.Unlock()

	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate != nil {
			// Serialize candidate writes after the offer that caused gathering.
			go func() {
				currentPeer.signalMu.Lock()
				defer currentPeer.signalMu.Unlock()
				_ = currentPeer.write("candidate", candidate.ToJSON())
			}()
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		slog.Info("peer state changed", "room", claims.Room, "peer", claims.ParticipantID, "state", state.String())
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateClosed {
			// Closing the signaling socket makes the handler leave the room and
			// perform the single, ordered PeerConnection cleanup.
			_ = currentPeer.ws.Close()
		}
	})
	pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		source := currentPeer.receiveSources[receiver]
		if source == "" {
			slog.Warn("received track on unknown transceiver", "room", claims.Room, "peer", claims.ParticipantID)
			return
		}
		currentRoom.forward(currentPeer, remote, source)
	})
	return currentPeer, nil
}

func (s *Server) leave(roomID, peerID string) {
	s.mu.RLock()
	currentRoom := s.rooms[roomID]
	s.mu.RUnlock()
	if currentRoom == nil {
		return
	}
	empty := currentRoom.removePeer(peerID)
	if empty {
		s.mu.Lock()
		if s.rooms[roomID] == currentRoom {
			currentRoom.mu.Lock()
			if len(currentRoom.peers) == 0 {
				delete(s.rooms, roomID)
			}
			currentRoom.mu.Unlock()
		}
		s.mu.Unlock()
	}
}

func (s *Server) dispatchKeyFrames() {
	s.mu.RLock()
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.mu.RUnlock()
	for _, current := range rooms {
		current.dispatchKeyFrames()
	}
}

func (r *room) forward(owner *peer, remote *webrtc.TrackRemote, source string) {
	// The track id is carried in SDP to subscribers. Keeping owner and source in
	// it lets the browser associate an arriving RTP track with the authenticated
	// participant snapshot without inventing a second track-signaling protocol.
	trackID := fmt.Sprintf("%s:%s:%d", owner.id, source, remote.SSRC())
	local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, trackID, owner.id)
	if err != nil {
		slog.Error("create local RTP track", "error", err)
		return
	}

	candidate := localTrack{ownerID: owner.id, source: source, track: local}
	r.mu.Lock()
	// A browser may replace camera/screen capture with a new RTP source while
	// keeping the negotiated transceiver. Only the newest uplink for a logical
	// source may be forwarded, otherwise a stale frozen camera can reappear next
	// to the replacement track.
	for id, existing := range r.uplinks {
		if existing.ownerID == owner.id && existing.source == source && id != trackID {
			delete(r.uplinks, id)
			delete(r.tracks, id)
		}
	}
	r.uplinks[trackID] = candidate
	if owner.sourceActive(source) {
		r.tracks[trackID] = candidate
	}
	r.mu.Unlock()
	r.sync()
	defer func() {
		r.mu.Lock()
		delete(r.uplinks, trackID)
		delete(r.tracks, trackID)
		r.mu.Unlock()
		r.sync()
	}()

	for {
		packet, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		packet.Extension = false
		packet.Extensions = nil
		if r.metrics != nil {
			r.metrics.forwardedPackets.Add(1)
			r.metrics.forwardedBytes.Add(uint64(packet.MarshalSize()))
		}
		if err := local.WriteRTP(packet); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			return
		}
	}
}

func (r *room) sync() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, currentPeer := range r.peers {
		if err := r.syncPeerLocked(currentPeer); err != nil {
			slog.Warn("renegotiation failed", "room", r.id, "peer", currentPeer.id, "error", err)
		}
	}
}

func (r *room) syncPeerLocked(currentPeer *peer) error {
	currentPeer.signalMu.Lock()
	defer currentPeer.signalMu.Unlock()
	if currentPeer.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return nil
	}
	if currentPeer.pc.SignalingState() != webrtc.SignalingStateStable {
		return nil
	}

	changed := false
	existing := make(map[string]bool)
	for _, sender := range currentPeer.pc.GetSenders() {
		track := sender.Track()
		if track == nil {
			continue
		}
		existing[track.ID()] = true
		candidate, present := r.tracks[track.ID()]
		if !present || candidate.ownerID == currentPeer.id {
			if err := currentPeer.pc.RemoveTrack(sender); err != nil {
				return err
			}
			changed = true
		}
	}
	for id, candidate := range r.tracks {
		if candidate.ownerID == currentPeer.id || existing[id] {
			continue
		}
		sender, err := currentPeer.pc.AddTrack(candidate.track)
		if err != nil {
			return err
		}
		go drainRTCP(sender)
		changed = true
	}
	if currentPeer.offered && !changed {
		return nil
	}

	offer, err := currentPeer.pc.CreateOffer(nil)
	if err != nil {
		return err
	}
	if err := currentPeer.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	currentPeer.offered = true
	tracks := make([]trackView, 0)
	for _, transceiver := range currentPeer.pc.GetTransceivers() {
		sender := transceiver.Sender()
		if sender == nil || sender.Track() == nil {
			continue
		}
		candidate, present := r.tracks[sender.Track().ID()]
		if !present || candidate.ownerID == currentPeer.id {
			continue
		}
		tracks = append(tracks, trackView{
			MID: transceiver.Mid(), TrackID: candidate.track.ID(),
			ParticipantID: candidate.ownerID, Source: candidate.source,
		})
	}
	if err := currentPeer.write("tracks", map[string]any{"tracks": tracks}); err != nil {
		return err
	}
	return currentPeer.write("offer", offer)
}

func (r *room) removePeer(peerID string) bool {
	r.mu.Lock()
	currentPeer := r.peers[peerID]
	delete(r.peers, peerID)
	for id, track := range r.uplinks {
		if track.ownerID == peerID {
			delete(r.uplinks, id)
		}
	}
	for id, track := range r.tracks {
		if track.ownerID == peerID {
			delete(r.tracks, id)
		}
	}
	empty := len(r.peers) == 0
	r.mu.Unlock()
	if currentPeer != nil {
		currentPeer.close()
	}
	if !empty {
		r.broadcastParticipants()
		r.sync()
	}
	return empty
}

func (r *room) removeUser(userID string) (int, bool) {
	r.mu.Lock()
	peerIDs := make([]string, 0)
	for peerID, currentPeer := range r.peers {
		if currentPeer.claims.UserID == userID {
			peerIDs = append(peerIDs, peerID)
		}
	}
	r.mu.Unlock()
	for _, peerID := range peerIDs {
		r.removePeer(peerID)
	}
	r.mu.Lock()
	empty := len(r.peers) == 0
	r.mu.Unlock()
	return len(peerIDs), empty
}

func (r *room) broadcastParticipants() {
	r.mu.Lock()
	peers := make([]*peer, 0, len(r.peers))
	participants := make([]participantView, 0, len(r.peers))
	for _, currentPeer := range r.peers {
		peers = append(peers, currentPeer)
		participants = append(participants, currentPeer.participantView())
	}
	r.mu.Unlock()

	for _, currentPeer := range peers {
		if err := currentPeer.write("participants", map[string]any{"participants": participants}); err != nil {
			slog.Debug("participant snapshot write failed", "room", r.id, "peer", currentPeer.id, "error", err)
		}
	}
}

func (r *room) applyPeerState(currentPeer *peer) {
	r.mu.Lock()
	changed := false
	for id, candidate := range r.uplinks {
		if candidate.ownerID != currentPeer.id {
			continue
		}
		_, active := r.tracks[id]
		shouldBeActive := currentPeer.sourceActive(candidate.source)
		if shouldBeActive && !active {
			r.tracks[id] = candidate
			changed = true
		} else if !shouldBeActive && active {
			delete(r.tracks, id)
			changed = true
		}
	}
	r.mu.Unlock()
	if changed {
		r.sync()
	}
}

func (r *room) dispatchKeyFrames() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, currentPeer := range r.peers {
		for _, receiver := range currentPeer.pc.GetReceivers() {
			track := receiver.Track()
			if track != nil && track.Kind() == webrtc.RTPCodecTypeVideo {
				_ = currentPeer.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(track.SSRC())}})
			}
		}
	}
}

func (r *room) close() {
	r.mu.Lock()
	peers := make([]*peer, 0, len(r.peers))
	for _, currentPeer := range r.peers {
		peers = append(peers, currentPeer)
	}
	r.peers = make(map[string]*peer)
	r.uplinks = make(map[string]localTrack)
	r.tracks = make(map[string]localTrack)
	r.mu.Unlock()
	for _, currentPeer := range peers {
		currentPeer.close()
	}
}

func (p *peer) handleSignal(message signalMessage) error {
	switch message.Event {
	case "answer":
		answer := webrtc.SessionDescription{}
		if err := json.Unmarshal(message.Data, &answer); err != nil {
			return errors.New("invalid WebRTC answer")
		}
		p.signalMu.Lock()
		err := p.pc.SetRemoteDescription(answer)
		if err == nil {
			for _, candidate := range p.pendingRemoteCandidates {
				if candidateErr := p.pc.AddICECandidate(candidate); candidateErr != nil {
					err = candidateErr
					break
				}
			}
			p.pendingRemoteCandidates = nil
		}
		p.signalMu.Unlock()
		if err != nil {
			return fmt.Errorf("apply WebRTC answer: %w", err)
		}
		p.room.sync()
		return nil
	case "candidate":
		candidate := webrtc.ICECandidateInit{}
		if err := json.Unmarshal(message.Data, &candidate); err != nil {
			return errors.New("invalid ICE candidate")
		}
		p.signalMu.Lock()
		if p.pc.RemoteDescription() == nil {
			p.pendingRemoteCandidates = append(p.pendingRemoteCandidates, candidate)
			p.signalMu.Unlock()
			return nil
		}
		err := p.pc.AddICECandidate(candidate)
		p.signalMu.Unlock()
		if err != nil {
			return fmt.Errorf("apply ICE candidate: %w", err)
		}
		return nil
	case "state":
		state := mediaState{}
		if err := json.Unmarshal(message.Data, &state); err != nil {
			return errors.New("invalid participant media state")
		}
		p.stateMu.Lock()
		p.state = state
		p.stateMu.Unlock()
		p.room.applyPeerState(p)
		p.room.broadcastParticipants()
		return nil
	default:
		return fmt.Errorf("unsupported signaling event %q", message.Event)
	}
}

func (p *peer) participantView() participantView {
	p.stateMu.RLock()
	state := p.state
	p.stateMu.RUnlock()
	return participantView{
		ParticipantID: p.id,
		UserID:        p.claims.UserID,
		Username:      p.claims.Username,
		DisplayName:   p.claims.DisplayName,
		AvatarURL:     p.claims.AvatarURL,
		MicMuted:      state.MicMuted,
		CameraOn:      state.CameraOn,
		ScreenOn:      state.ScreenOn,
	}
}

func (p *peer) sourceActive(source string) bool {
	if source == "microphone" {
		return true
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	if source == "camera" {
		return p.state.CameraOn
	}
	if source == "screen" {
		return p.state.ScreenOn
	}
	return false
}

func (p *peer) write(event string, data any) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_ = p.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return p.ws.WriteJSON(map[string]any{"event": event, "data": data})
}

func (p *peer) ping() error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	_ = p.ws.SetWriteDeadline(deadline)
	return p.ws.WriteControl(websocket.PingMessage, nil, deadline)
}

func (p *peer) allowSignal() bool {
	now := time.Now()
	p.rateMu.Lock()
	defer p.rateMu.Unlock()
	if p.rateWindowStarted.IsZero() || now.Sub(p.rateWindowStarted) >= signalRateWindow {
		p.rateWindowStarted = now
		p.rateWindowCount = 0
	}
	p.rateWindowCount++
	return p.rateWindowCount <= maxSignalsPerWindow
}

func drainRTCP(sender *webrtc.RTPSender) {
	buffer := make([]byte, 1500)
	for {
		if _, _, err := sender.Read(buffer); err != nil {
			return
		}
	}
}

func (p *peer) close() {
	p.closed.Do(func() {
		_ = p.pc.Close()
		_ = p.ws.Close()
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func boolMetric(value bool) int {
	if value {
		return 1
	}
	return 0
}

func writeCounter(w http.ResponseWriter, name string, value uint64) {
	_, _ = fmt.Fprintf(w, "# TYPE %s counter\n%s %d\n", name, name, value)
}
