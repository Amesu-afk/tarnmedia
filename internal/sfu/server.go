package sfu

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

const maxSignalMessageBytes = 1 << 20

type Server struct {
	cfg      config.Config
	api      *webrtc.API
	pcConfig webrtc.Configuration
	upgrader websocket.Upgrader

	mu    sync.RWMutex
	rooms map[string]*room
}

type signalMessage struct {
	Event string          `json:"event"`
	Data  json.RawMessage `json:"data,omitempty"`
}

type authenticateData struct {
	Token string `json:"token"`
}

type localTrack struct {
	ownerID string
	track   *webrtc.TrackLocalStaticRTP
}

type room struct {
	id       string
	maxPeers int
	mu       sync.Mutex
	peers    map[string]*peer
	tracks   map[string]localTrack
}

type peer struct {
	id     string
	claims auth.Claims
	room   *room
	pc     *webrtc.PeerConnection
	ws     *websocket.Conn

	writeMu                 sync.Mutex
	signalMu                sync.Mutex
	closed                  sync.Once
	offered                 bool
	pendingRemoteCandidates []webrtc.ICECandidateInit
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
		pcConfig: pcConfig,
		rooms:    make(map[string]*room),
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

func (s *Server) RunMaintenance(stop <-chan struct{}) {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.dispatchKeyFrames()
		case <-stop:
			return
		}
	}
}

func (s *Server) Close() {
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
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready", "service": "tarnmedia"})
}

func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	conn.SetReadLimit(maxSignalMessageBytes)
	_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))

	claims, err := s.authenticate(conn)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	currentPeer, err := s.join(claims, conn)
	if err != nil {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseTryAgainLater, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	defer s.leave(claims.Room, currentPeer.id)

	_ = currentPeer.write("authenticated", map[string]any{
		"room":          claims.Room,
		"participantId": claims.ParticipantID,
		"userId":        claims.UserID,
	})
	currentPeer.room.sync()

	for {
		message := signalMessage{}
		if err := conn.ReadJSON(&message); err != nil {
			return
		}
		if err := currentPeer.handleSignal(message); err != nil {
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
	return auth.Parse(data.Token, s.cfg.JWTSecret)
}

func (s *Server) join(claims auth.Claims, conn *websocket.Conn) (*peer, error) {
	pc, err := s.api.NewPeerConnection(s.pcConfig)
	if err != nil {
		return nil, fmt.Errorf("create PeerConnection: %w", err)
	}
	currentPeer := &peer{id: claims.ParticipantID, claims: claims, pc: pc, ws: conn}

	for _, kind := range []webrtc.RTPCodecType{
		webrtc.RTPCodecTypeAudio,
		webrtc.RTPCodecTypeVideo,
		webrtc.RTPCodecTypeVideo,
	} {
		if _, err := pc.AddTransceiverFromKind(kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly}); err != nil {
			_ = pc.Close()
			return nil, fmt.Errorf("add receiving transceiver: %w", err)
		}
	}

	s.mu.Lock()
	currentRoom := s.rooms[claims.Room]
	if currentRoom == nil {
		currentRoom = &room{
			id: claims.Room, maxPeers: s.cfg.MaxPeersPerRoom,
			peers: make(map[string]*peer), tracks: make(map[string]localTrack),
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
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		currentRoom.forward(currentPeer, remote)
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

func (r *room) forward(owner *peer, remote *webrtc.TrackRemote) {
	trackID := fmt.Sprintf("%s:%s:%d", owner.id, remote.ID(), remote.SSRC())
	local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, trackID, owner.id)
	if err != nil {
		slog.Error("create local RTP track", "error", err)
		return
	}

	r.mu.Lock()
	r.tracks[trackID] = localTrack{ownerID: owner.id, track: local}
	r.mu.Unlock()
	r.sync()
	defer func() {
		r.mu.Lock()
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
	return currentPeer.write("offer", offer)
}

func (r *room) removePeer(peerID string) bool {
	r.mu.Lock()
	currentPeer := r.peers[peerID]
	delete(r.peers, peerID)
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
		r.sync()
	}
	return empty
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
	default:
		return fmt.Errorf("unsupported signaling event %q", message.Event)
	}
}

func (p *peer) write(event string, data any) error {
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_ = p.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return p.ws.WriteJSON(map[string]any{"event": event, "data": data})
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
