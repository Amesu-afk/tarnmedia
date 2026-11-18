package sfu

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/ice/v4"
	"github.com/pion/interceptor"
	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

// guardPanic keeps one broken callback from crashing the whole SFU. Pion invokes
// callbacks and our readers on goroutines we do not own, so an unexpected panic
// there would otherwise take down every active call in the process.
func guardPanic(where string) {
	if recovered := recover(); recovered != nil {
		slog.Error("recovered panic", "where", where, "panic", recovered, "stack", string(debug.Stack()))
	}
}

const (
	maxSignalMessageBytes  = 1 << 20
	maxControlMessageBytes = 8 << 10
	signalRateWindow       = 10 * time.Second
	maxSignalsPerWindow    = 240
	websocketPongWait      = 45 * time.Second
	websocketPingPeriod    = 15 * time.Second
	revocationRetention    = 10 * time.Minute
	// A key frame costs roughly ten delta frames and is paid for by the publisher's
	// uplink and by every subscriber. In a full room a burst of subscribers can each
	// ask for one at the same moment, so requests are collapsed per uplink: one key
	// frame answers all of them.
	keyFrameMinInterval = 500 * time.Millisecond
	// A stalled ICE path is usually a NAT rebinding or a network switch, not a
	// participant who left. Spend an ICE restart before dropping the call: the
	// browser answers the restarted offer and both sides gather fresh
	// candidates under the same signaling socket.
	//
	// The delay is the silence a broken path costs before the FIRST attempt, so
	// the shorter it is, the sooner the call comes back. The budget counts REAL
	// attempts only: an attempt burnt while a renegotiation is in flight does
	// not count, see recoverICE.
	iceRestartDelay    = 3 * time.Second
	iceRestartAttempts = 3
	// How many times recovery may be postponed because a renegotiation is in
	// flight. Such a renegotiation carries its own candidates, so waiting for it
	// is better than spending the restart budget on a no-op.
	iceRecoveryDefers = 3
	// A publisher that stops sending RTP while the room is willing to forward its
	// audio is the first thing to know about a one-way-audio call. Checked often
	// enough to be useful in incident logs, rare enough to stay cheap.
	mediaStallCheckPeriod = 15 * time.Second
	mediaStallTimeout     = 10 * time.Second
)

type serverMetrics struct {
	websocketConnections atomic.Uint64
	authFailures         atomic.Uint64
	rejectedConnections  atomic.Uint64
	signalErrors         atomic.Uint64
	signalRateLimited    atomic.Uint64
	forwardedPackets     atomic.Uint64
	forwardedBytes       atomic.Uint64
	forwardWriteFailures atomic.Uint64
	// Packets dropped because a subscriber's bounded queue was full. This is the
	// operational signal of a slow transport (per-subscriber backpressure).
	subscriberDrops     atomic.Uint64
	controlRequests     atomic.Uint64
	controlAuthFailures atomic.Uint64
	evictedPeers        atomic.Uint64
	keyFramesRequested  atomic.Uint64
	keyFramesCollapsed  atomic.Uint64
	// Completed RID layer switches across all subscribers.
	simulcastSwitches atomic.Uint64
	// REMB feedback received from subscribers; zero on a browser/transport that
	// relies on TWCC only, which is why layer selection also uses queue pressure.
	rembReceived atomic.Uint64
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
	// Один UDP-порт на весь процесс: ICE-мультиплексор разбирает пакеты по ufrag.
	udpMux   ice.UDPMux
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
	// publish marks a client that negotiates its media slots as the offerer.
	// Without it the server keeps the original behaviour: it offers recvonly
	// slots itself. The field is additive, so older clients stay on that path.
	Publish bool `json:"publish"`
	// simulcast asks for RID layers. It is only meaningful together with
	// publish and only honoured when TARNMEDIA_SIMULCAST is on.
	Simulcast bool `json:"simulcast"`
}

// clientHello is what authenticate may negotiate beyond the token.
type clientHello struct {
	publish   bool
	simulcast bool
}

// publishOfferData is the client's offer for its own media slots. midToSource
// replaces the server's old m-line-order convention: the publisher is the only
// side that knows which transceiver carries which source after it creates them.
type publishOfferData struct {
	SDP         webrtc.SessionDescription `json:"sdp"`
	MidToSource map[string]string         `json:"midToSource"`
}

type mediaState struct {
	MicMuted        bool `json:"micMuted"`
	MicTransmitting bool `json:"micTransmitting"`
	CameraOn        bool `json:"cameraOn"`
	ScreenOn        bool `json:"screenOn"`
}

// incomingMediaState retains whether a newer client explicitly reported its
// effective PTT/VAD transmission state. Older clients omit MicTransmitting.
type incomingMediaState struct {
	MicMuted        bool  `json:"micMuted"`
	MicTransmitting *bool `json:"micTransmitting"`
	CameraOn        bool  `json:"cameraOn"`
	ScreenOn        bool  `json:"screenOn"`
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
	// owner, ssrc and keyFrames route a subscriber's key frame request back to the
	// publisher that can satisfy it. keyFrames is nil for audio, which never needs one.
	owner     *peer
	ssrc      webrtc.SSRC
	keyFrames *keyFrameGate
	// simulcast is set on a logical video track fed by RID layers. The room maps
	// and SDP carry one localTrack per (owner, source); the layers live inside.
	simulcast *simulcastSource
	// Shared with the forwarding loop (the struct itself is copied into the room
	// maps), so the stall watchdog sees live packet timing.
	activity *trackActivity
}

// trackActivity carries the timing the stall watchdog needs. Every field is
// atomic because the forwarding goroutine and the maintenance ticker both touch
// it through copies of localTrack.
type trackActivity struct {
	lastPacket atomic.Int64 // unix nanoseconds of the last RTP packet read
	stalled    atomic.Bool
}

// Per-subscriber queues are deliberately short. The queue exists to absorb a
// scheduling hiccup, not to buffer seconds of video: a consumer that cannot keep
// up should lose packets (and get a key frame) instead of adding latency.
const (
	subscriberVideoQueue = 32
	subscriberAudioQueue = 16
)

// subscriberSink is one subscriber's private copy of a published track.
// The publisher's read loop only enqueues an immutable packet slice; a dedicated
// worker writes it to that subscriber's TrackLocalStaticRTP. A blocked or broken
// transport therefore cannot delay packets for other subscribers.
type subscriberSink struct {
	room      *room
	trackID   string
	peerID    string
	track     *webrtc.TrackLocalStaticRTP
	queue     chan []byte
	done      chan struct{}
	closeOnce sync.Once
	upstream  localTrack
	video     bool
	metrics   *serverMetrics
	// Set for a simulcast video source: one logical track, per-subscriber layer
	// choice, and sequence/timestamp continuity across switches.
	selector  *layerSelector
	rewriter  *rtpRewriter
	rewriteMu sync.Mutex
	// Last observed layer-set version and time, to avoid rebuilding availability
	// per packet while still noticing a silently disabled encoding.
	layersVersion uint64
	layersChecked time.Time
}

func (s *subscriberSink) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func (s *subscriberSink) push(data []byte) {
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case s.queue <- data:
	case <-s.done:
	default:
		s.recordDrop()
	}
}

// recordDrop accounts for a packet lost to a full queue and asks for a key
// frame, since the subscriber's decoder has no way to recover the gap.
func (s *subscriberSink) recordDrop() {
	if s.metrics != nil {
		s.metrics.subscriberDrops.Add(1)
	}
	if s.selector != nil {
		s.selector.noteDrop(time.Now())
	}
	// Video recovers from a key frame; request one at most as often as the
	// key frame gate allows. Audio has no equivalent, so its queue is longer
	// and a drop there is a genuinely overloaded consumer.
	if s.video {
		s.requestKeyFrame(s.metrics)
	}
}

// pushLayer forwards one packet of a RID layer. Only the layer this
// subscriber currently watches is rewritten and queued; the others are read
// and dropped, because the publisher necessarily uploads all of them.
func (s *subscriberSink) pushLayer(rid string, packet *rtp.Packet) {
	if s.selector == nil {
		s.pushMarshaled(packet)
		return
	}
	select {
	case <-s.done:
		return
	default:
	}

	now := time.Now()
	s.rewriteMu.Lock()
	if version := s.upstream.simulcast.layerVersion(); version != s.layersVersion ||
		now.Sub(s.layersChecked) >= simulcastLayerSilence/4 {
		s.layersVersion = version
		s.layersChecked = now
		s.selector.setAvailable(s.upstream.simulcast.layerRIDs())
	}
	requestRID := ""
	if target, wanted := s.selector.nextSwitch(now); wanted {
		requestRID = target
	}
	send, switched := s.selector.accept(rid, isKeyFrame(packet, s.upstream.simulcast.mimeType), now)
	if !send {
		s.rewriteMu.Unlock()
		if requestRID != "" {
			s.upstream.simulcast.requestKeyFrame(requestRID, s.metrics)
		}
		return
	}
	clone := packet.Clone()
	// RID/MID extensions describe the publisher's simulcast m-line, not this
	// subscriber's single-layer downlink. Carrying them over would only confuse
	// the receiver.
	clone.Extension = false
	clone.Extensions = nil
	if switched {
		s.rewriter.beginLayerSwitch()
		if s.metrics != nil {
			s.metrics.simulcastSwitches.Add(1)
		}
		slog.Debug("simulcast switch", "track", s.trackID, "subscriber", s.peerID, "to", rid)
	}
	s.rewriter.rewrite(clone)
	data, err := clone.Marshal()
	s.rewriteMu.Unlock()
	if err != nil {
		return
	}
	s.pushMarshaledBytes(data)
	if requestRID != "" {
		s.upstream.simulcast.requestKeyFrame(requestRID, s.metrics)
	}
}

func (s *subscriberSink) pushMarshaled(packet *rtp.Packet) {
	data, err := packet.Marshal()
	if err != nil {
		return
	}
	s.pushMarshaledBytes(data)
}

func (s *subscriberSink) pushMarshaledBytes(data []byte) {
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case s.queue <- data:
	case <-s.done:
	default:
		s.recordDrop()
	}
}

func (s *subscriberSink) requestKeyFrame(metrics *serverMetrics) {
	if s.upstream.simulcast != nil {
		s.upstream.simulcast.requestKeyFrame(s.selector.priorityRID(), metrics)
		return
	}
	s.upstream.requestKeyFrame(metrics)
}

func (s *subscriberSink) run() {
	defer guardPanic("subscriber-sink")
	packet := &rtp.Packet{}
	for {
		select {
		case <-s.done:
			return
		case data := <-s.queue:
			if err := packet.Unmarshal(data); err != nil {
				continue
			}
			if err := s.track.WriteRTP(packet); err != nil {
				if !errors.Is(err, io.ErrClosedPipe) && s.metrics != nil {
					s.metrics.forwardWriteFailures.Add(1)
				}
				s.close()
				s.room.dropSink(s.trackID, s.peerID, s)
				return
			}
		}
	}
}

// keyFrameGate collapses key frame requests for one uplink into at most one per
// keyFrameMinInterval.
type keyFrameGate struct {
	mu   sync.Mutex
	last time.Time
}

func (g *keyFrameGate) allow(now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.last.IsZero() && now.Sub(g.last) < keyFrameMinInterval {
		return false
	}
	g.last = now
	return true
}

// requestKeyFrame asks the publisher of this uplink for a key frame. Subscribers ask
// for one when they cannot decode; the SFU does not decode and cannot tell on its own.
func (t localTrack) requestKeyFrame(metrics *serverMetrics) {
	if t.simulcast != nil {
		t.simulcast.requestDefaultKeyFrame(metrics)
		return
	}
	if t.keyFrames == nil || t.owner == nil || t.owner.pc == nil {
		return
	}
	if !t.keyFrames.allow(time.Now()) {
		if metrics != nil {
			metrics.keyFramesCollapsed.Add(1)
		}
		return
	}
	if metrics != nil {
		metrics.keyFramesRequested.Add(1)
	}
	_ = t.owner.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(t.ssrc)}})
}

type room struct {
	id       string
	maxPeers int
	// simulcast mirrors the server flag; layers are the shared RID thresholds.
	simulcast bool
	layers    []layerLimit
	mu        sync.Mutex
	peers     map[string]*peer
	uplinks   map[string]localTrack
	tracks    map[string]localTrack
	// trackID -> subscriber peerID -> private sink. Every subscriber gets its own
	// TrackLocalStaticRTP and a bounded queue, so one slow transport cannot stall
	// the publisher's read loop for everyone else.
	subscriptions map[string]map[string]*subscriberSink
	metrics       *serverMetrics
}

type peer struct {
	id             string
	claims         auth.Claims
	room           *room
	pc             *webrtc.PeerConnection
	ws             *websocket.Conn
	receiveMu      sync.RWMutex
	receiveSources map[*webrtc.RTPReceiver]string
	// publish marks a client that offered its own media slots (simulcast) and
	// publishReady marks the answer that completed that handshake. Downstream
	// offers must wait for publishReady: two offers crossing on one transport
	// is a glare neither side can resolve.
	publisher    bool
	publishReady bool
	// simulcast says the publisher asked for RID layers. A publish client that
	// did not (Android WebView) is still accepted, just as a single layer.
	simulcast bool

	writeMu  sync.Mutex
	signalMu sync.Mutex
	stateMu  sync.RWMutex
	state    mediaState
	closed   sync.Once
	offered  bool
	// A track snapshot arrived while SDP was busy. Guarded by signalMu, like
	// the negotiation state it belongs to; the answer handler repeats the sync.
	syncDirty               bool
	pendingRemoteCandidates []webrtc.ICECandidateInit
	// ICE recovery state. Touched from the connection-state callback, the
	// signaling loop and the recovery timer, so everything is under recoveryMu.
	recoveryMu    sync.Mutex
	recoveryTimer *time.Timer
	iceRestarts   int
	// How many times recovery was postponed because a renegotiation was in
	// flight. Postponements do not spend the restart budget.
	iceDefers int
	// Video uplinks this peer subscribed to in the offer it has not answered yet.
	// Guarded by signalMu, like the negotiation state it belongs to.
	pendingKeyFrames  []localTrack
	rateMu            sync.Mutex
	rateWindowStarted time.Time
	rateWindowCount   int
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
	// Simulcast needs the MID and RID header extensions on the receiving side:
	// without them Pion cannot attribute an RTP stream to a layer, and OnTrack
	// arrives with an empty RID.
	if err := webrtc.ConfigureSimulcastExtensionHeaders(mediaEngine); err != nil {
		return nil, fmt.Errorf("register simulcast header extensions: %w", err)
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(mediaEngine, interceptors); err != nil {
		return nil, fmt.Errorf("register WebRTC interceptors: %w", err)
	}
	settings := webrtc.SettingEngine{}
	// Все PeerConnection делят один UDP-порт. Раньше каждый занимал порт из
	// диапазона в 100 штук, и исчерпание диапазона ломало вход новым участникам
	// во ВСЕХ комнатах. Порт 0 (в тестах) означает «выбери свободный».
	udpConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: int(cfg.UDPPort)})
	if err != nil {
		return nil, fmt.Errorf("listen UDP mux on port %d: %w", cfg.UDPPort, err)
	}
	udpMux := ice.NewUDPMuxDefault(ice.UDPMuxParams{UDPConn: udpConn})
	settings.SetICEUDPMux(udpMux)
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
		udpMux:      udpMux,
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
	mux.HandleFunc("GET /v1/capabilities", s.handleCapabilities)
	mux.HandleFunc("POST /v1/control", s.handleControl)
	return mux
}

func (s *Server) SetReady(ready bool) {
	s.ready.Store(ready)
}

func (s *Server) RunMaintenance(stop <-chan struct{}) {
	pingTicker := time.NewTicker(websocketPingPeriod)
	stallTicker := time.NewTicker(mediaStallCheckPeriod)
	cleanupTicker := time.NewTicker(time.Minute)
	defer pingTicker.Stop()
	defer stallTicker.Stop()
	defer cleanupTicker.Stop()
	// A panic in one maintenance pass must not take down signaling for every call.
	safe := func(where string, job func()) {
		defer guardPanic(where)
		job()
	}
	for {
		select {
		case <-pingTicker.C:
			safe("maintenance-ping", s.pingPeers)
		case <-stallTicker.C:
			safe("maintenance-stall", func() { s.checkMediaStalls(time.Now()) })
		case <-cleanupTicker.C:
			safe("maintenance-cleanup", s.cleanupRevocations)
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
	if s.udpMux != nil {
		_ = s.udpMux.Close()
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
	writeCounter(w, "tarnmedia_forward_write_failures_total", s.metrics.forwardWriteFailures.Load())
	writeCounter(w, "tarnmedia_subscriber_drops_total", s.metrics.subscriberDrops.Load())
	writeCounter(w, "tarnmedia_control_requests_total", s.metrics.controlRequests.Load())
	writeCounter(w, "tarnmedia_control_auth_failures_total", s.metrics.controlAuthFailures.Load())
	writeCounter(w, "tarnmedia_evicted_peers_total", s.metrics.evictedPeers.Load())
	writeCounter(w, "tarnmedia_key_frames_requested_total", s.metrics.keyFramesRequested.Load())
	writeCounter(w, "tarnmedia_key_frames_collapsed_total", s.metrics.keyFramesCollapsed.Load())
	writeCounter(w, "tarnmedia_simulcast_switches_total", s.metrics.simulcastSwitches.Load())
	writeCounter(w, "tarnmedia_remb_received_total", s.metrics.rembReceived.Load())
}

func (s *Server) handleCapabilities(w http.ResponseWriter, r *http.Request) {
	if !s.controlAuthorized(r.Header.Get("Authorization")) {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"maxPeersPerRoom": s.cfg.MaxPeersPerRoom})
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
	slog.Info("media control applied", "action", command.Action, "room", command.Room, "user", command.UserID, "affectedPeers", affected)
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
				slog.Warn("signaling ping failed", "room", current.id, "peer", currentPeer.id, "reason", signalingCloseReason(err), "state", currentPeer.pc.ConnectionState().String())
				_ = currentPeer.ws.Close()
			}
		}
	}
}

func revocationKey(roomID, userID string) string {
	return roomID + "\x00" + userID
}

// checkMediaStalls reports publishers whose RTP stopped arriving while the room
// is still willing to forward their audio. This is the difference between "the
// call is fine but nobody speaks" and "this participant went silent", which is
// otherwise indistinguishable in the logs.
func (s *Server) checkMediaStalls(now time.Time) {
	s.mu.RLock()
	rooms := make([]*room, 0, len(s.rooms))
	for _, current := range s.rooms {
		rooms = append(rooms, current)
	}
	s.mu.RUnlock()
	for _, current := range rooms {
		current.checkMediaStalls(now)
	}
}

func (r *room) checkMediaStalls(now time.Time) {
	r.mu.Lock()
	uplinks := make([]localTrack, 0, len(r.uplinks))
	for _, candidate := range r.uplinks {
		uplinks = append(uplinks, candidate)
	}
	r.mu.Unlock()

	for _, candidate := range uplinks {
		if candidate.activity == nil || candidate.owner == nil {
			continue
		}
		// A muted or PTT/VAD-gated publisher legitimately sends nothing, and the
		// room is not forwarding it anyway: that silence is not a stall.
		if !candidate.owner.allowsPacketForwarding(candidate.source) {
			candidate.activity.stalled.Store(false)
			continue
		}
		last := candidate.activity.lastPacket.Load()
		if last == 0 {
			continue
		}
		silent := now.Sub(time.Unix(0, last))
		if silent <= mediaStallTimeout {
			if candidate.activity.stalled.Swap(false) {
				slog.Info("publisher media resumed", "room", r.id, "publisher", candidate.ownerID, "source", candidate.source)
			}
			continue
		}
		// One line per episode, not one per tick.
		if !candidate.activity.stalled.Swap(true) {
			slog.Warn("publisher media stalled", "room", r.id, "publisher", candidate.ownerID, "source", candidate.source, "silentFor", silent.Round(time.Second).String())
		}
	}
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

	claims, hello, err := s.authenticate(conn)
	if err != nil {
		s.metrics.authFailures.Add(1)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, err.Error()), time.Now().Add(time.Second))
		_ = conn.Close()
		return
	}
	_ = conn.SetReadDeadline(time.Time{})

	currentPeer, err := s.join(claims, hello, conn)
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
		// publish advertises the client-offer handshake; an older client ignores
		// it and keeps answering the server's recvonly offer.
		"publish":   s.cfg.Simulcast,
		"simulcast": s.cfg.Simulcast,
	})
	currentPeer.room.broadcastParticipants()
	// A publishing peer gets its downlink offer only after publish_answer; syncPeer
	// remembers this call in syncDirty until the publish handshake completes.
	currentPeer.room.sync()

	for {
		message := signalMessage{}
		if err := conn.ReadJSON(&message); err != nil {
			slog.Info("signaling closed", "room", claims.Room, "peer", currentPeer.id, "reason", signalingCloseReason(err), "state", currentPeer.pc.ConnectionState().String())
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

// Record bounded transport categories, never client-supplied close text or SDP.
func signalingCloseReason(err error) string {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return fmt.Sprintf("websocket-%d", closeErr.Code)
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return "timeout"
	}
	if errors.Is(err, net.ErrClosed) {
		return "socket-closed"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	return "transport-or-protocol-error"
}
func (s *Server) authenticate(conn *websocket.Conn) (auth.Claims, clientHello, error) {
	message := signalMessage{}
	if err := conn.ReadJSON(&message); err != nil {
		return auth.Claims{}, clientHello{}, errors.New("authentication message required")
	}
	if message.Event != "authenticate" {
		return auth.Claims{}, clientHello{}, errors.New("first message must be authenticate")
	}
	data := authenticateData{}
	if err := json.Unmarshal(message.Data, &data); err != nil || data.Token == "" {
		return auth.Claims{}, clientHello{}, errors.New("media token required")
	}
	claims, err := auth.Parse(data.Token, s.cfg.JWTSecret, s.cfg.Issuer)
	if err != nil {
		return auth.Claims{}, clientHello{}, err
	}
	if err := s.validateSession(claims); err != nil {
		return auth.Claims{}, clientHello{}, err
	}
	// The publish handshake is part of the simulcast feature. With the feature
	// off the server behaves exactly as before, even if a newer client asks.
	hello := clientHello{}
	if s.cfg.Simulcast {
		hello.publish = data.Publish
		hello.simulcast = data.Publish && data.Simulcast
	}
	return claims, hello, nil
}

var (
	// The API could not answer (down, busy, 5xx). Worth one more try: a brief
	// hiccup must not turn into a failed join.
	errValidationUnavailable = errors.New("media session validation is unavailable")
	// The API answered and refused (ban, password change, revoked session).
	// Fail closed immediately; retrying only delays an inevitable rejection.
	errSessionInactive = errors.New("media session is no longer active")
)

// validateSession keeps the SFU stateless with respect to user accounts while
// still making a JWT revocable after a password change or ban. The endpoint
// is loopback-only in normal deployment and requires the control secret.
func (s *Server) validateSession(claims auth.Claims) error {
	payload, err := json.Marshal(struct {
		UserID         string `json:"userId"`
		SessionVersion int    `json:"sessionVersion"`
	}{UserID: claims.UserID, SessionVersion: claims.SessionVersion})
	if err != nil {
		return errValidationUnavailable
	}
	// Two attempts total. The call is a read-only check, so repeating it is safe.
	for attempt := 0; attempt < 2; attempt++ {
		err := s.requestSessionValidation(payload)
		if err == nil || !errors.Is(err, errValidationUnavailable) {
			return err
		}
	}
	return errValidationUnavailable
}

func (s *Server) requestSessionValidation(payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, s.cfg.AuthURL, bytes.NewReader(payload))
	if err != nil {
		return errValidationUnavailable
	}
	req.Header.Set("Authorization", "Bearer "+s.cfg.ControlSecret)
	req.Header.Set("Content-Type", "application/json")
	response, err := s.authHTTP.Do(req)
	if err != nil {
		return errValidationUnavailable
	}
	// Drain before closing so the connection can be reused for the retry.
	_, _ = io.Copy(io.Discard, response.Body)
	_ = response.Body.Close()
	if response.StatusCode == http.StatusNoContent {
		return nil
	}
	if response.StatusCode >= http.StatusInternalServerError {
		return errValidationUnavailable
	}
	return errSessionInactive
}

func (s *Server) join(claims auth.Claims, hello clientHello, conn *websocket.Conn) (*peer, error) {
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
		// Block microphone RTP until the client reports its initial media state.
		state:        mediaState{MicMuted: true, MicTransmitting: false},
		publisher:    hello.publish,
		publishReady: !hello.publish,
		simulcast:    hello.simulcast,
	}

	if !hello.publish {
		for _, input := range []struct {
			kind   webrtc.RTPCodecType
			source string
		}{
			{kind: webrtc.RTPCodecTypeAudio, source: "microphone"},
			{kind: webrtc.RTPCodecTypeVideo, source: "camera"},
			{kind: webrtc.RTPCodecTypeVideo, source: "screen"},
			// The screen's own sound travels on its own slot rather than mixed into
			// the microphone: mute must silence the voice without silencing what is
			// being shown, and the publisher's noise suppression must never touch it.
			// This slot is deliberately LAST. The client answers the offer built from
			// this list and picks its publish slots by m-line order, so appending
			// keeps clients that predate the slot working against a newer server.
			{kind: webrtc.RTPCodecTypeAudio, source: "screenAudio"},
		} {
			transceiver, err := pc.AddTransceiverFromKind(input.kind, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionRecvonly})
			if err != nil {
				_ = pc.Close()
				return nil, fmt.Errorf("add receiving transceiver: %w", err)
			}
			currentPeer.receiveSources[transceiver.Receiver()] = input.source
		}
	}

	s.mu.Lock()
	currentRoom := s.rooms[claims.Room]
	if currentRoom == nil {
		layers := make([]layerLimit, 0, len(s.cfg.SimulcastLayers))
		for _, layer := range s.cfg.SimulcastLayers {
			layers = append(layers, layerLimit{rid: layer.RID, bitrate: layer.Bitrate})
		}
		if len(layers) == 0 {
			layers = defaultLayerLimits()
		}
		currentRoom = &room{
			id: claims.Room, maxPeers: s.cfg.MaxPeersPerRoom,
			simulcast: s.cfg.Simulcast, layers: layers,
			peers:   make(map[string]*peer),
			uplinks: make(map[string]localTrack), tracks: make(map[string]localTrack),
			subscriptions: make(map[string]map[string]*subscriberSink),
			metrics:       &s.metrics,
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
		// Gathering and teardown both invoke this; a panic here must not stop
		// the SFU for everyone else.
		defer guardPanic("ice-candidate")
		if candidate != nil {
			// Serialize candidate writes after the offer that caused gathering.
			go func() {
				defer guardPanic("ice-candidate-write")
				currentPeer.signalMu.Lock()
				defer currentPeer.signalMu.Unlock()
				_ = currentPeer.write("candidate", candidate.ToJSON())
			}()
		}
	})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		defer guardPanic("connection-state")
		slog.Info("peer state changed", "room", claims.Room, "peer", claims.ParticipantID, "state", state.String())
		switch state {
		case webrtc.PeerConnectionStateConnected:
			currentPeer.settleICERecovery()
		case webrtc.PeerConnectionStateDisconnected, webrtc.PeerConnectionStateFailed:
			// Wait out a brief stall before spending a renegotiation: a short
			// hiccup recovers on its own, and only a path that stays broken is
			// worth new ICE credentials.
			currentPeer.scheduleICERecovery()
		case webrtc.PeerConnectionStateClosed:
			// Closing the signaling socket makes the handler leave the room and
			// perform the single, ordered PeerConnection cleanup.
			_ = currentPeer.ws.Close()
		}
	})
	pc.OnTrack(func(remote *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		// forward blocks for the lifetime of the track; recovering here keeps a
		// malformed RTP path from killing every other call.
		defer guardPanic("on-track")
		currentPeer.receiveMu.RLock()
		source := currentPeer.receiveSources[receiver]
		currentPeer.receiveMu.RUnlock()
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

func (r *room) forward(owner *peer, remote *webrtc.TrackRemote, source string) {
	if r.simulcast && owner.simulcast && remote.RID() != "" && (source == "camera" || source == "screen") {
		r.forwardLayer(owner, remote, source)
		return
	}
	// The track id is carried in SDP to subscribers. Keeping owner and source in
	// it lets the browser associate an arriving RTP track with the authenticated
	// participant snapshot without inventing a second track-signaling protocol.
	trackID := fmt.Sprintf("%s:%s:%d", owner.id, source, remote.SSRC())
	local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, trackID, owner.id)
	if err != nil {
		slog.Error("create local RTP track", "error", err)
		return
	}

	activity := &trackActivity{}
	candidate := localTrack{
		ownerID: owner.id, source: source, track: local,
		owner: owner, ssrc: remote.SSRC(), activity: activity,
	}
	if remote.Kind() == webrtc.RTPCodecTypeVideo {
		candidate.keyFrames = &keyFrameGate{}
	}
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
		// Recorded before the mute gate: the watchdog measures the publisher's
		// stream, not whether this room decided to forward it.
		activity.lastPacket.Store(time.Now().UnixNano())
		// MediaStreamTrack.enabled is the first mute barrier, but some WebView /
		// device combinations can keep producing RTP while that state propagates.
		// Do not let manual mute or the effective PTT/VAD policy feed speaker audio
		// back to the other participants. Keep the sender and its negotiated track
		// alive: removing it here would force an SDP renegotiation on every toggle.
		if !owner.allowsPacketForwarding(source) {
			continue
		}
		packet.Extension = false
		packet.Extensions = nil
		data, err := packet.Marshal()
		if err != nil {
			continue
		}
		if r.metrics != nil {
			r.metrics.forwardedPackets.Add(1)
			r.metrics.forwardedBytes.Add(uint64(len(data)))
		}
		// Fan-out is per-subscriber: a blocked transport drains its own queue and
		// loses packets, never the publisher's read loop.
		r.broadcast(trackID, data)
	}
}

// forwardLayer reads one RID encoding of a logical video track. Several of
// these loops run for one publisher (q/h/f); they all feed the same localTrack,
// and each subscriber picks the layer it forwards.
func (r *room) forwardLayer(owner *peer, remote *webrtc.TrackRemote, source string) {
	rid := remote.RID()
	ssrc := remote.SSRC()
	slog.Debug("simulcast layer", "owner", owner.id, "source", source, "rid", rid, "ssrc", ssrc)

	r.mu.Lock()
	trackID := ""
	candidate := localTrack{}
	found := false
	for id, existing := range r.uplinks {
		if existing.ownerID == owner.id && existing.source == source && existing.simulcast != nil {
			trackID, candidate, found = id, existing, true
			break
		}
	}
	if !found {
		// A RID-less uplink for the same source is stale as soon as a layer
		// arrives: the publisher now negotiates simulcast.
		for id, existing := range r.uplinks {
			if existing.ownerID == owner.id && existing.source == source && existing.simulcast == nil {
				delete(r.uplinks, id)
				delete(r.tracks, id)
			}
		}
		trackID = fmt.Sprintf("%s:%s:%d", owner.id, source, ssrc)
		local, err := webrtc.NewTrackLocalStaticRTP(remote.Codec().RTPCodecCapability, trackID, owner.id)
		if err != nil {
			r.mu.Unlock()
			slog.Error("create local simulcast RTP track", "error", err)
			return
		}
		activity := &trackActivity{}
		candidate = localTrack{
			ownerID: owner.id, source: source, track: local,
			owner: owner, ssrc: ssrc, activity: activity,
		}
		candidate.simulcast = newSimulcastSource(owner, remote.Codec().MimeType)
		r.uplinks[trackID] = candidate
		if owner.sourceActive(source) {
			r.tracks[trackID] = candidate
		}
	}
	activity := candidate.activity
	sourceTrack := candidate.simulcast
	layer := sourceTrack.addLayer(rid, ssrc, remote)
	r.mu.Unlock()
	if !found {
		r.sync()
	}
	defer func() {
		r.mu.Lock()
		empty := sourceTrack.removeLayer(rid, ssrc)
		if current, ok := r.uplinks[trackID]; ok && current.simulcast == sourceTrack {
			if empty {
				delete(r.uplinks, trackID)
				delete(r.tracks, trackID)
			}
		}
		r.mu.Unlock()
		// A disappearing encoding can be the one a subscriber was watching;
		// sync lets its selector settle on another without waiting for SDP.
		r.sync()
	}()

	for {
		packet, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		layer.lastPacket.Store(time.Now().UnixNano())
		activity.lastPacket.Store(time.Now().UnixNano())
		if !owner.allowsPacketForwarding(source) {
			continue
		}
		if r.metrics != nil {
			r.metrics.forwardedPackets.Add(1)
			r.metrics.forwardedBytes.Add(uint64(packet.MarshalSize()))
		}
		r.broadcastLayer(trackID, rid, packet)
	}
}

func (r *room) broadcastLayer(trackID, rid string, packet *rtp.Packet) {
	r.mu.Lock()
	byTrack := r.subscriptions[trackID]
	sinks := make([]*subscriberSink, 0, len(byTrack))
	for _, sink := range byTrack {
		sinks = append(sinks, sink)
	}
	r.mu.Unlock()
	for _, sink := range sinks {
		sink.pushLayer(rid, packet)
	}
}

func (r *room) broadcast(trackID string, data []byte) {
	r.mu.Lock()
	byTrack := r.subscriptions[trackID]
	sinks := make([]*subscriberSink, 0, len(byTrack))
	for _, sink := range byTrack {
		sinks = append(sinks, sink)
	}
	r.mu.Unlock()
	for _, sink := range sinks {
		sink.push(data)
	}
}

func (r *room) dropSink(trackID, peerID string, expected *subscriberSink) {
	var removed *subscriberSink
	r.mu.Lock()
	if byTrack := r.subscriptions[trackID]; byTrack != nil {
		current := byTrack[peerID]
		if current != nil && (expected == nil || expected == current) {
			removed = current
			delete(byTrack, peerID)
			if len(byTrack) == 0 {
				delete(r.subscriptions, trackID)
			}
		}
	}
	r.mu.Unlock()
	if removed != nil {
		removed.close()
	}
}

// dropPeerSinks removes every subscription of one peer. The caller must not hold
// r.mu.
func (r *room) dropPeerSinks(peerID string) {
	var sinks []*subscriberSink
	r.mu.Lock()
	for trackID, byTrack := range r.subscriptions {
		if sink := byTrack[peerID]; sink != nil {
			delete(byTrack, peerID)
			sinks = append(sinks, sink)
		}
		if len(byTrack) == 0 {
			delete(r.subscriptions, trackID)
		}
	}
	r.mu.Unlock()
	for _, sink := range sinks {
		sink.close()
	}
}

// ensureSink returns this subscriber's own track for a published source,
// creating the track, queue and worker on first use.
func (r *room) ensureSink(currentPeer *peer, candidate localTrack) (*subscriberSink, error) {
	trackID := candidate.track.ID()
	r.mu.Lock()
	if byTrack := r.subscriptions[trackID]; byTrack != nil {
		if existing := byTrack[currentPeer.id]; existing != nil {
			r.mu.Unlock()
			return existing, nil
		}
	}
	r.mu.Unlock()

	track, err := webrtc.NewTrackLocalStaticRTP(candidate.track.Codec(), trackID, candidate.track.StreamID())
	if err != nil {
		return nil, err
	}
	video := candidate.source == "camera" || candidate.source == "screen"
	queueSize := subscriberAudioQueue
	if video {
		queueSize = subscriberVideoQueue
	}
	sink := &subscriberSink{
		room: r, trackID: trackID, peerID: currentPeer.id, track: track,
		queue: make(chan []byte, queueSize), done: make(chan struct{}),
		upstream: candidate, video: video, metrics: r.metrics,
	}
	if video && candidate.simulcast != nil {
		limits := r.layers
		if len(limits) == 0 {
			limits = defaultLayerLimits()
		}
		sink.selector = newLayerSelector(limits, candidate.simulcast.layerRIDs(), "h", time.Now())
		slog.Debug("simulcast sink", "track", trackID, "subscriber", currentPeer.id, "layers", candidate.simulcast.layerRIDs(), "start", sink.selector.current)
		// Stable per-subscriber SSRC, sequence and timestamp across layer
		// switches. The sender's own binding also fixes an SSRC, but sequence
		// and timestamp continuity are ours to maintain.
		sink.rewriter = newRTPRewriter(randomOutSSRC())
	}

	r.mu.Lock()
	if byTrack := r.subscriptions[trackID]; byTrack != nil {
		if existing := byTrack[currentPeer.id]; existing != nil {
			r.mu.Unlock()
			sink.close()
			return existing, nil
		}
	} else {
		r.subscriptions[trackID] = make(map[string]*subscriberSink)
	}
	r.subscriptions[trackID][currentPeer.id] = sink
	r.mu.Unlock()

	go sink.run()
	return sink, nil
}

func (r *room) sync() {
	// The room lock is held only while the lists are copied. Writing to a
	// signaling socket can wait for a stuck client for up to five seconds, and
	// doing that under r.mu froze the whole room: new uplinks, departures and
	// camera toggles all waited for the slowest subscriber.
	r.mu.Lock()
	peers := make([]*peer, 0, len(r.peers))
	for _, currentPeer := range r.peers {
		peers = append(peers, currentPeer)
	}
	tracks := make(map[string]localTrack, len(r.tracks))
	for id, candidate := range r.tracks {
		tracks[id] = candidate
	}
	r.mu.Unlock()

	for _, currentPeer := range peers {
		if err := r.syncPeer(currentPeer, tracks); err != nil {
			slog.Warn("renegotiation failed", "room", r.id, "peer", currentPeer.id, "error", err)
		}
	}
}

// syncPeer brings one subscriber's senders in line with the given track
// snapshot. Callers must not hold the room lock (see sync); the per-peer
// signaling lock serializes concurrent negotiations instead.
func (r *room) syncPeer(currentPeer *peer, available map[string]localTrack) error {
	currentPeer.signalMu.Lock()
	defer currentPeer.signalMu.Unlock()
	if currentPeer.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return nil
	}
	if currentPeer.pc.SignalingState() != webrtc.SignalingStateStable {
		// The snapshot is still valid, but SDP cannot change while a
		// negotiation is in flight. Remember it: without this flag the track
		// change was silently lost until some unrelated room event happened to
		// trigger another sync — heard as a one-off silence at call start.
		currentPeer.syncDirty = true
		return nil
	}
	if !currentPeer.publishReady {
		// The publisher's own offer/answer is still being negotiated. Starting a
		// downstream offer now would cross it (glare); the publish handler syncs
		// once its answer is on the wire.
		currentPeer.syncDirty = true
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
		candidate, present := available[track.ID()]
		if !present || candidate.ownerID == currentPeer.id {
			if err := currentPeer.pc.RemoveTrack(sender); err != nil {
				return err
			}
			r.dropSink(track.ID(), currentPeer.id, nil)
			changed = true
		}
	}
	for id, candidate := range available {
		if candidate.ownerID == currentPeer.id || existing[id] {
			continue
		}
		sink, err := r.ensureSink(currentPeer, candidate)
		if err != nil {
			return err
		}
		sender, err := currentPeer.pc.AddTrack(sink.track)
		if err != nil {
			r.dropSink(id, currentPeer.id, sink)
			return err
		}
		go func() {
			defer guardPanic("subscriber-rtcp")
			readSubscriberRTCP(sender, sink, r.metrics)
		}()
		if candidate.keyFrames != nil {
			currentPeer.pendingKeyFrames = append(currentPeer.pendingKeyFrames, candidate)
		}
		changed = true
	}
	if currentPeer.offered && !changed {
		return nil
	}
	if !currentPeer.offered && !changed && currentPeer.publisher {
		// The publish handshake already established this transport. An empty
		// downstream offer would negotiate no media and only risk a glare.
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
		candidate, present := available[sender.Track().ID()]
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
	if err := currentPeer.write("offer", offer); err != nil {
		return err
	}
	currentPeer.syncDirty = false
	return nil
}

func (r *room) removePeer(peerID string) bool {
	r.mu.Lock()
	currentPeer := r.peers[peerID]
	delete(r.peers, peerID)
	var sinks []*subscriberSink
	for trackID, byTrack := range r.subscriptions {
		if sink := byTrack[peerID]; sink != nil {
			delete(byTrack, peerID)
			sinks = append(sinks, sink)
		}
		if len(byTrack) == 0 {
			delete(r.subscriptions, trackID)
		}
	}
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
	for _, sink := range sinks {
		sink.close()
	}
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

func (r *room) close() {
	r.mu.Lock()
	peers := make([]*peer, 0, len(r.peers))
	for _, currentPeer := range r.peers {
		peers = append(peers, currentPeer)
	}
	var sinks []*subscriberSink
	for _, byTrack := range r.subscriptions {
		for _, sink := range byTrack {
			sinks = append(sinks, sink)
		}
	}
	r.peers = make(map[string]*peer)
	r.uplinks = make(map[string]localTrack)
	r.tracks = make(map[string]localTrack)
	r.subscriptions = make(map[string]map[string]*subscriberSink)
	r.mu.Unlock()
	for _, sink := range sinks {
		sink.close()
	}
	for _, currentPeer := range peers {
		currentPeer.close()
	}
}

func (p *peer) handleSignal(message signalMessage) error {
	switch message.Event {
	case "publish_offer":
		return p.handlePublishOffer(message.Data)
	case "answer":
		answer := webrtc.SessionDescription{}
		if err := json.Unmarshal(message.Data, &answer); err != nil {
			return errors.New("invalid WebRTC answer")
		}
		p.signalMu.Lock()
		err := p.pc.SetRemoteDescription(answer)
		var subscribed []localTrack
		if err == nil {
			for _, candidate := range p.pendingRemoteCandidates {
				if candidateErr := p.pc.AddICECandidate(candidate); candidateErr != nil {
					err = candidateErr
					break
				}
			}
			p.pendingRemoteCandidates = nil
		}
		if err == nil {
			// Hand the queue over only once the answer is fully applied. A failed
			// negotiation leaves it in place for the next one.
			subscribed, p.pendingKeyFrames = p.pendingKeyFrames, nil
		}
		p.signalMu.Unlock()
		if err != nil {
			return fmt.Errorf("apply WebRTC answer: %w", err)
		}
		p.room.sync()
		// A sync that landed while the SDP was busy was remembered but had no
		// fresh snapshot at that moment. Repeat it now that signaling is stable.
		p.signalMu.Lock()
		dirty := p.syncDirty
		p.syncDirty = false
		p.signalMu.Unlock()
		if dirty {
			p.room.sync()
		}
		// The answer is the first moment this subscriber can decode what it was just
		// offered, and it subscribed between the publisher's own key frames. Only the
		// newly subscribed uplinks are asked: renegotiation also happens on every mute
		// toggle, and those must not cost the room a round of key frames.
		for _, candidate := range subscribed {
			candidate.requestKeyFrame(p.room.metrics)
		}
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
		state, err := decodeMediaState(message.Data)
		if err != nil {
			return errors.New("invalid participant media state")
		}
		p.stateMu.Lock()
		previous := p.state
		p.state = state
		p.stateMu.Unlock()
		// Clients repeat their state after every reconnect and on every control
		// touch. A full participant snapshot for everyone when nothing changed is
		// pure noise, and the signaling budget is finite.
		if previous == state {
			return nil
		}
		p.room.applyPeerState(p)
		p.room.broadcastParticipants()
		return nil
	default:
		return fmt.Errorf("unsupported signaling event %q", message.Event)
	}
}

// handlePublishOffer applies the publisher's own offer and answers it only
// after ICE gathering, so the client never has to wait for a second round of
// trickled candidates to complete the transport. The answer completes the
// publish handshake; only then may the room offer downstream tracks.
func (p *peer) handlePublishOffer(data json.RawMessage) error {
	offer := publishOfferData{}
	if err := json.Unmarshal(data, &offer); err != nil {
		return errors.New("invalid publish offer")
	}
	if offer.SDP.Type != webrtc.SDPTypeOffer || offer.SDP.SDP == "" {
		return errors.New("publish offer must be a WebRTC offer")
	}
	if len(offer.MidToSource) == 0 {
		return errors.New("publish offer must map at least one media slot")
	}
	if !p.publisher {
		return errors.New("publish offer requires publish negotiation")
	}

	p.signalMu.Lock()
	if p.pc.SignalingState() != webrtc.SignalingStateStable {
		p.signalMu.Unlock()
		return errors.New("publish offer arrived while another negotiation is in flight")
	}
	if err := p.pc.SetRemoteDescription(offer.SDP); err != nil {
		p.signalMu.Unlock()
		return fmt.Errorf("apply publish offer: %w", err)
	}

	transceivers := make(map[string]*webrtc.RTPTransceiver, len(p.pc.GetTransceivers()))
	for _, transceiver := range p.pc.GetTransceivers() {
		if mid := transceiver.Mid(); mid != "" {
			transceivers[mid] = transceiver
		}
	}
	sources := make(map[*webrtc.RTPReceiver]string, len(offer.MidToSource))
	used := make(map[string]bool, len(offer.MidToSource))
	for mid, source := range offer.MidToSource {
		transceiver := transceivers[mid]
		if transceiver == nil {
			p.signalMu.Unlock()
			return fmt.Errorf("publish offer maps unknown mid %q", mid)
		}
		receiver := transceiver.Receiver()
		if receiver == nil {
			p.signalMu.Unlock()
			return fmt.Errorf("publish offer maps a mid without a receiver: %q", mid)
		}
		kind, known := publishSourceKind(source)
		if !known {
			p.signalMu.Unlock()
			return fmt.Errorf("unknown publish source %q", source)
		}
		if used[source] {
			p.signalMu.Unlock()
			return fmt.Errorf("publish source %q is mapped twice", source)
		}
		if transceiver.Kind() != kind {
			p.signalMu.Unlock()
			return fmt.Errorf("publish source %q does not match its m-line kind", source)
		}
		used[source] = true
		sources[receiver] = source
	}
	p.receiveMu.Lock()
	p.receiveSources = sources
	p.receiveMu.Unlock()

	// Candidates that arrived before this offer were queued; the remote
	// description is the first moment they can be applied. A single stale
	// candidate must not fail the handshake, the next one will arrive.
	for _, candidate := range p.pendingRemoteCandidates {
		_ = p.pc.AddICECandidate(candidate)
	}
	p.pendingRemoteCandidates = nil

	answer, err := p.pc.CreateAnswer(nil)
	if err != nil {
		p.signalMu.Unlock()
		return fmt.Errorf("create publish answer: %w", err)
	}
	gathered := webrtc.GatheringCompletePromise(p.pc)
	if err := p.pc.SetLocalDescription(answer); err != nil {
		p.signalMu.Unlock()
		return fmt.Errorf("apply publish answer: %w", err)
	}
	// The answer is sent as one complete SDP with every gathered candidate.
	// This is required by the spike: a trickled answer raced the client's own
	// offer and left the transport without a usable candidate pair.
	<-gathered
	local := p.pc.LocalDescription()
	if local == nil {
		p.signalMu.Unlock()
		return errors.New("publish answer was not created")
	}
	p.publishReady = true
	err = p.write("publish_answer", local)
	p.signalMu.Unlock()
	if err != nil {
		return fmt.Errorf("send publish answer: %w", err)
	}
	// Downstream offers may start now. The room may already have tracks from
	// other publishers; the subscriber side of this peer is negotiated as usual.
	p.room.sync()
	return nil
}

func publishSourceKind(source string) (webrtc.RTPCodecType, bool) {
	switch source {
	case "microphone", "screenAudio":
		return webrtc.RTPCodecTypeAudio, true
	case "camera", "screen":
		return webrtc.RTPCodecTypeVideo, true
	default:
		return 0, false
	}
}

func decodeMediaState(data json.RawMessage) (mediaState, error) {
	incoming := incomingMediaState{}
	if err := json.Unmarshal(data, &incoming); err != nil {
		return mediaState{}, err
	}

	// A state message from an already-shipped client has no micTransmitting
	// field. Treat it as transmitting to preserve its pre-upgrade behavior;
	// newly joined peers are still fail-closed before any state is received.
	state := mediaState{
		MicMuted:        incoming.MicMuted,
		MicTransmitting: true,
		CameraOn:        incoming.CameraOn,
		ScreenOn:        incoming.ScreenOn,
	}
	if incoming.MicTransmitting != nil {
		state.MicTransmitting = *incoming.MicTransmitting
	}
	return state, nil
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
	if source == "screen" || source == "screenAudio" {
		return p.state.ScreenOn
	}
	return false
}

// allowsPacketForwarding is deliberately separate from sourceActive. A
// microphone remains negotiated so it can resume immediately, but no RTP from
// it may reach other peers unless the participant is both unmuted and actively
// transmitting. MicTransmitting defaults to false, so a newly joined peer
// cannot leak audio before its client reports the effective PTT/VAD state.
func (p *peer) allowsPacketForwarding(source string) bool {
	if source != "microphone" {
		return true
	}
	p.stateMu.RLock()
	defer p.stateMu.RUnlock()
	return !p.state.MicMuted && p.state.MicTransmitting
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

// scheduleICERecovery waits out a short stall before spending an ICE restart.
// Only one timer is armed at a time: the state callback fires on every broken
// sample while the path stays down.
func (p *peer) scheduleICERecovery() {
	p.recoveryMu.Lock()
	defer p.recoveryMu.Unlock()
	if p.recoveryTimer != nil {
		return
	}
	// The state callback can still fire while the room tears this peer down.
	if p.pc == nil || p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return
	}
	p.recoveryTimer = time.AfterFunc(iceRestartDelay, p.recoverICE)
}

func (p *peer) cancelICERecovery() {
	p.recoveryMu.Lock()
	if p.recoveryTimer != nil {
		p.recoveryTimer.Stop()
		p.recoveryTimer = nil
	}
	p.recoveryMu.Unlock()
}

// settleICERecovery runs once the transport is healthy again: the spent budget
// is returned so a later blip still gets its full allowance.
func (p *peer) settleICERecovery() {
	p.cancelICERecovery()
	p.recoveryMu.Lock()
	p.iceRestarts = 0
	p.iceDefers = 0
	p.recoveryMu.Unlock()
}

func (p *peer) recoverICE() {
	defer guardPanic("ice-recovery")
	p.recoveryMu.Lock()
	p.recoveryTimer = nil
	attempts := p.iceRestarts
	defers := p.iceDefers
	p.recoveryMu.Unlock()

	if p.pc == nil || p.ws == nil {
		return
	}
	state := p.pc.ConnectionState()
	if state == webrtc.PeerConnectionStateConnected || state == webrtc.PeerConnectionStateClosed {
		// The path recovered on its own. The budget stays untouched: settleICERecovery
		// resets it when the state change arrives, and an attempt spent on a path
		// that is already healthy is an attempt spent for nothing.
		return
	}

	// A renegotiation is already in flight. It carries its own candidates, so an
	// ICE restart cannot be applied right now — and it must NOT cost a restart
	// from the budget: the old code counted such no-ops, so two collisions with
	// an unrelated renegotiation closed the peer without a single real attempt.
	if p.pc.SignalingState() != webrtc.SignalingStateStable {
		if defers >= iceRecoveryDefers {
			slog.Warn("ICE recovery postponed too often", "peer", p.id, "state", state.String())
			_ = p.ws.Close()
			return
		}
		p.recoveryMu.Lock()
		p.iceDefers++
		p.recoveryMu.Unlock()
		p.scheduleICERecovery()
		return
	}

	if attempts >= iceRestartAttempts {
		slog.Warn("ICE recovery exhausted", "peer", p.id, "state", state.String())
		// Closing the signaling socket leaves the room through the single,
		// ordered cleanup in the WebSocket handler, like any other goodbye.
		_ = p.ws.Close()
		return
	}

	p.recoveryMu.Lock()
	p.iceDefers = 0
	p.iceRestarts++
	p.recoveryMu.Unlock()

	if err := p.restartICE(); err != nil {
		slog.Warn("ICE restart failed", "peer", p.id, "error", err)
	}
	p.scheduleICERecovery()
}

// restartICE renegotiates the transport with fresh ICE credentials. This is the
// only way to revive a stalled pair without losing the participant: the browser
// is the answerer here, so it can only restart ICE after an offer from this
// side, and applying such an offer moves it out of failed back to connecting.
func (p *peer) restartICE() error {
	// The offer must be written while signalMu is held: the candidate callback
	// serializes gathering behind it, and candidates for the new generation are
	// useless (and rejected) before the offer that introduces them.
	p.signalMu.Lock()
	defer p.signalMu.Unlock()
	if p.pc.ConnectionState() == webrtc.PeerConnectionStateClosed {
		return nil
	}
	if p.pc.SignalingState() != webrtc.SignalingStateStable {
		// A renegotiation is already in flight. It carries its own candidates,
		// so there is nothing to add here and no reason to fail the recovery.
		return nil
	}
	offer, err := p.pc.CreateOffer(&webrtc.OfferOptions{ICERestart: true})
	if err != nil {
		return err
	}
	if err := p.pc.SetLocalDescription(offer); err != nil {
		return err
	}
	return p.write("offer", offer)
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

// readSubscriberRTCP consumes the RTCP a subscriber sends back about one forwarded
// track. Everything the interceptors already handle (receiver reports, NACK) is
// dropped here; a key frame request is the one thing only the publisher can answer,
// so it is passed upstream. REMB is the bandwidth evidence for layer selection.
// Reading must continue regardless: an unread stream eventually stalls the sender.
func readSubscriberRTCP(sender *webrtc.RTPSender, sink *subscriberSink, metrics *serverMetrics) {
	// 1500 bytes was too small for a compound RTCP packet from a subscriber in a
	// busy room: Read then returned a short-buffer error, this reader exited, and
	// PLI/FIR never reached the publisher again until the next renegotiation.
	buffer := make([]byte, 8192)
	for {
		n, _, err := sender.Read(buffer)
		if err != nil {
			return
		}
		packets, err := rtcp.Unmarshal(buffer[:n])
		if err != nil {
			continue
		}
		for _, packet := range packets {
			switch report := packet.(type) {
			case *rtcp.ReceiverEstimatedMaximumBitrate:
				if sink.selector != nil && report.Bitrate > 0 {
					sink.selector.setREMB(uint32(report.Bitrate))
					if metrics != nil {
						metrics.rembReceived.Add(1)
					}
				}
			case *rtcp.ReceiverReport:
				if sink.selector != nil {
					worst := 0.0
					for _, reception := range report.Reports {
						fractionLost := float64(reception.FractionLost) / 256
						if fractionLost > worst {
							worst = fractionLost
						}
					}
					sink.selector.noteReport(worst, time.Now())
				}
			}
		}
		if sink.video && requestsKeyFrame(packets) {
			sink.requestKeyFrame(metrics)
		}
	}
}

func requestsKeyFrame(packets []rtcp.Packet) bool {
	for _, packet := range packets {
		switch packet.(type) {
		case *rtcp.PictureLossIndication, *rtcp.FullIntraRequest:
			return true
		}
	}
	return false
}

func (p *peer) close() {
	p.closed.Do(func() {
		// A pending recovery attempt must not touch a closed transport.
		p.cancelICERecovery()
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
