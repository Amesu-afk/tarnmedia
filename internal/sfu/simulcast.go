package sfu

import (
	"math/rand/v2"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
)

// Layer selection is intentionally conservative: this is not congestion
// control, only a first, predictable step. Bandwidth evidence comes from REMB;
// queue overflow from a subscriber counts as immediate pressure. The timings
// are deliberate:
//
//   - a downgrade is allowed as soon as the 2 s guard has passed, because a
//     broken link should not wait for the next key frame;
//   - an upgrade waits for a full stable window and a key frame, because
//     moving to a bigger layer costs the publisher's uplink and every
//     subscriber shares that decision's consequences.
const (
	simulcastSwitchInterval = 2 * time.Second
	simulcastGrowAfter      = 8 * time.Second
	// A fresh subscription may learn its RID set one encoding at a time: Pion
	// probes SSRCs as packets arrive, so f can be registered before h and q.
	// Within this window the selector aligns to the preferred start layer even
	// if that means moving down from a probe-time upper layer.
	simulcastInitialRampWindow = 2 * time.Second
	// Fraction of the reported REMB a layer may use. The rest is headroom for
	// audio, RTCP and the burst a key frame creates.
	simulcastREMBRatio = 9 // out of 10
	// Chrome does not send REMB in every mode; it may rely on TWCC only. When
	// REMB is absent, receiver reports are the bandwidth evidence: grow after a
	// stable window without loss and fall back on real loss.
	simulcastLossDrop = 0.05
	simulcastLossGrow = 0.01
	// A layer that stopped producing packets is not a layer a subscriber should
	// be moved to. Two seconds is longer than any key-frame wait and shorter
	// than a user noticing a frozen picture.
	simulcastLayerSilence = 2 * time.Second
	// A layer that had to be abandoned is not retried immediately; otherwise a
	// link that fits h but not f would pump between them every few seconds.
	simulcastLayerCooldown = 30 * time.Second
	// One 30 fps frame at 90 kHz. Used only to keep the output timeline moving
	// when the source timestamp jumps to another encoder's clock.
	simulcastTimestampStep = 3000
)

// layerLimit is one configurable RID layer, ordered by bitrate.
type layerLimit struct {
	rid     string
	bitrate uint32
}

// defaultLayerLimits mirrors TARNMEDIA_SIMULCAST_LAYERS defaults and keeps
// rooms built by tests usable without config.
func defaultLayerLimits() []layerLimit {
	return []layerLimit{
		{rid: "q", bitrate: 150_000},
		{rid: "h", bitrate: 400_000},
		{rid: "f", bitrate: 1_200_000},
	}
}

// simulcastLayer is one RID encoding received from a publisher. All layers of
// one logical track arrive on the same transceiver (same receiver, same source)
// but as separate TrackRemote values with their own SSRC.
type simulcastLayer struct {
	rid    string
	ssrc   webrtc.SSRC
	remote *webrtc.TrackRemote
	// lastPacket keeps a silently disabled encoding out of layer selection
	// without removing the whole source.
	lastPacket atomic.Int64
	// One gate per layer: a switch to a layer asks that layer's encoder only.
	keyFrames keyFrameGate
}

// simulcastSource is one logical video track fed by several RID layers.
// Subscribers see one TrackLocalStaticRTP per source, never per layer.
type simulcastSource struct {
	mimeType string
	owner    *peer
	// version changes whenever the set of RIDs changes, so a per-packet path can
	// refresh subscriber availability without allocating a slice every packet.
	version atomic.Uint64
	mu      sync.RWMutex
	layers  map[string]*simulcastLayer
}

func newSimulcastSource(owner *peer, mimeType string) *simulcastSource {
	return &simulcastSource{
		mimeType: mimeType, owner: owner,
		layers: make(map[string]*simulcastLayer),
	}
}

// addLayer registers a RID encoding. A re-published RID (the browser replaced
// its camera source without renegotiating) replaces the old reader.
func (s *simulcastSource) addLayer(rid string, ssrc webrtc.SSRC, remote *webrtc.TrackRemote) *simulcastLayer {
	layer := &simulcastLayer{rid: rid, ssrc: ssrc, remote: remote}
	layer.lastPacket.Store(time.Now().UnixNano())
	s.mu.Lock()
	s.layers[rid] = layer
	s.mu.Unlock()
	s.version.Add(1)
	return layer
}

// removeLayer removes one encoding only if it is still the current one for that
// RID. A newer TrackRemote for the same RID must not be evicted by the old
// reader's cleanup. It reports whether the logical source became empty.
func (s *simulcastSource) removeLayer(rid string, ssrc webrtc.SSRC) bool {
	s.mu.Lock()
	current, present := s.layers[rid]
	removed := present && current.ssrc == ssrc
	if removed {
		delete(s.layers, rid)
	}
	empty := len(s.layers) == 0
	s.mu.Unlock()
	if removed {
		s.version.Add(1)
	}
	return empty
}

func (s *simulcastSource) layerVersion() uint64 {
	return s.version.Load()
}

// layerRIDs returns the RIDs that are currently arriving. A stale encoding is
// excluded after simulcastLayerSilence; it may resume later without touching
// the rest of the source.
func (s *simulcastSource) layerRIDs() []string {
	now := time.Now()
	s.mu.RLock()
	defer s.mu.RUnlock()
	rids := make([]string, 0, len(s.layers))
	for rid, layer := range s.layers {
		last := layer.lastPacket.Load()
		if last == 0 || now.Sub(time.Unix(0, last)) <= simulcastLayerSilence {
			rids = append(rids, rid)
		}
	}
	return rids
}

func (s *simulcastSource) layer(rid string) *simulcastLayer {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.layers[rid]
}

// requestKeyFrame asks the publisher's encoder for a key frame on exactly one
// RID. The gate collapses the burst of requests a room produces when several
// subscribers switch at the same moment.
func (s *simulcastSource) requestKeyFrame(rid string, metrics *serverMetrics) {
	layer := s.layer(rid)
	if layer == nil || layer.remote == nil {
		return
	}
	owner := s.owner
	if owner == nil || owner.pc == nil {
		return
	}
	if !layer.keyFrames.allow(time.Now()) {
		if metrics != nil {
			metrics.keyFramesCollapsed.Add(1)
		}
		return
	}
	if metrics != nil {
		metrics.keyFramesRequested.Add(1)
	}
	_ = owner.pc.WriteRTCP([]rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: uint32(layer.ssrc)}})
}

func (s *simulcastSource) requestDefaultKeyFrame(metrics *serverMetrics) {
	for _, rid := range []string{"h", "f", "q"} {
		if s.layer(rid) != nil {
			s.requestKeyFrame(rid, metrics)
			return
		}
	}
	rids := s.layerRIDs()
	if len(rids) > 0 {
		s.requestKeyFrame(rids[0], metrics)
	}
}

// layerSelector decides which RID one subscriber receives. It is deliberately
// independent from the packet path so its hysteresis can be tested with a fake
// clock.
type layerSelector struct {
	mu        sync.Mutex
	limits    []layerLimit // ascending bitrate
	available map[string]bool
	current   string
	target    string
	waiting   bool
	// startRID is the layer a fresh subscription prefers. If it appears after a
	// smaller layer (Chrome starts q before h), the selector promotes to it
	// without waiting for a full growth window.
	startRID  string
	startedAt time.Time
	remb      uint32
	loss      float64
	// blockedAbove is the layer that had to be abandoned and its retry deadline.
	blockedAbove string
	blockedUntil time.Time
	lastDrop     time.Time
	lastSwitch   time.Time
	stableSince  time.Time
}

func newLayerSelector(limits []layerLimit, available []string, preferred string, now time.Time) *layerSelector {
	selector := &layerSelector{limits: limits, stableSince: now, startRID: preferred, startedAt: now}
	selector.available = make(map[string]bool, len(available))
	for _, rid := range available {
		selector.available[rid] = true
	}
	selector.current = selector.startLayer(preferred)
	return selector
}

func (s *layerSelector) startLayer(preferred string) string {
	if s.available[preferred] {
		return preferred
	}
	preferredRank := s.rankOf(preferred)
	best := ""
	for _, limit := range s.limits {
		if !s.available[limit.rid] {
			continue
		}
		if preferredRank >= 0 && s.rankOf(limit.rid) <= preferredRank {
			best = limit.rid
		}
	}
	if best != "" {
		return best
	}
	// The preferred layer is missing entirely: take the highest available.
	for i := len(s.limits) - 1; i >= 0; i-- {
		if s.available[s.limits[i].rid] {
			return s.limits[i].rid
		}
	}
	return ""
}

func (s *layerSelector) rankOf(rid string) int {
	for index, limit := range s.limits {
		if limit.rid == rid {
			return index
		}
	}
	return -1
}

// setAvailable mirrors the layers that are actually on the wire. A layer can
// stop without the source disappearing: the browser may pause an encoding.
func (s *layerSelector) setAvailable(rids []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(rids) == len(s.available) {
		same := true
		for _, rid := range rids {
			if !s.available[rid] {
				same = false
				break
			}
		}
		if same {
			return
		}
	}
	s.available = make(map[string]bool, len(rids))
	for _, rid := range rids {
		s.available[rid] = true
	}
	if s.current == "" {
		s.current = s.startLayer("h")
	}
	if s.waiting && !s.available[s.target] {
		s.waiting = false
		s.target = ""
	}
}

func (s *layerSelector) noteDrop(now time.Time) {
	s.mu.Lock()
	s.noteDropLocked(now)
	s.mu.Unlock()
}

// noteDropLocked records congestion pressure. The layer we are leaving is not
// retried for a cooldown: a link that fits h but not f must not pump f every
// few seconds.
func (s *layerSelector) noteDropLocked(now time.Time) {
	if s.current != "" && s.rankOf(s.current) > 0 {
		s.blockedAbove = s.current
		s.blockedUntil = now.Add(simulcastLayerCooldown)
	}
	s.lastDrop = now
	s.stableSince = now
}

// noteReport consumes the fraction lost from a subscriber's receiver report.
// Chrome may not send REMB at all; RR loss is then the only bandwidth evidence
// besides our own queue.
func (s *layerSelector) noteReport(fractionLost float64, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if fractionLost < 0 {
		fractionLost = 0
	}
	s.loss = fractionLost
	if fractionLost >= simulcastLossDrop {
		s.noteDropLocked(now)
	}
}

func (s *layerSelector) setREMB(bits uint32) {
	s.mu.Lock()
	s.remb = bits
	s.mu.Unlock()
}

// nextSwitch returns the layer a switch should move to, if one is due. The
// caller requests a key frame and keeps forwarding the current layer until it
// arrives.
func (s *layerSelector) nextSwitch(now time.Time) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.current == "" {
		s.current = s.startLayer("h")
		if s.current == "" {
			return "", false
		}
		return "", false
	}
	if s.waiting {
		return "", false
	}
	if !s.available[s.current] {
		target := s.fallbackLayer()
		if target == "" || target == s.current {
			return "", false
		}
		s.beginSwitch(target, now)
		return target, true
	}
	// Align a subscription that was created from a partial RID set (see the
	// constant). This can be a downgrade: the first probed SSRC is often f.
	if s.lastSwitch.IsZero() && now.Sub(s.startedAt) < simulcastInitialRampWindow &&
		s.available[s.startRID] && s.current != s.startRID {
		s.beginSwitch(s.startRID, now)
		return s.startRID, true
	}
	if !s.lastSwitch.IsZero() && now.Sub(s.lastSwitch) < simulcastSwitchInterval {
		return "", false
	}
	wanted := s.wantedLayer(now)
	if wanted == "" || wanted == s.current {
		return "", false
	}
	initialRamp := wanted == s.startRID && s.rankOf(s.current) < s.rankOf(s.startRID)
	if s.rankOf(wanted) > s.rankOf(s.current) && !initialRamp && now.Sub(s.stableSince) < simulcastGrowAfter {
		return "", false
	}
	s.beginSwitch(wanted, now)
	return wanted, true
}

func (s *layerSelector) beginSwitch(rid string, now time.Time) {
	s.target = rid
	s.waiting = true
	s.lastSwitch = now
}

// accept reports whether a packet of this layer may go out, and whether it is
// the first packet of a completed switch (an aligned key frame).
func (s *layerSelector) accept(rid string, keyFrame bool, now time.Time) (send bool, switched bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiting {
		if rid == s.target {
			if !keyFrame {
				return false, false
			}
			switched = rid != s.current
			s.current = rid
			s.waiting = false
			s.target = ""
			s.lastSwitch = now
			s.stableSince = now
			return true, switched
		}
		// Keep the current layer alive until the target key frame arrives: a
		// freeze during the switch is worse than a few more packets at the old
		// layer's bitrate.
		if rid == s.current {
			return true, false
		}
		return false, false
	}
	if rid == s.current {
		return true, false
	}
	return false, false
}

// priorityRID is the layer a key frame request should target: the one being
// switched to, otherwise the one currently forwarded.
func (s *layerSelector) priorityRID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.waiting && s.target != "" {
		return s.target
	}
	return s.current
}

func (s *layerSelector) wantedLayer(now time.Time) string {
	if s.remb == 0 {
		// No REMB: Chrome may rely on TWCC only. Grow one step after a stable,
		// lossless window; any real loss or queue drop already reset the window.
		wanted := s.current
		healthy := s.loss <= simulcastLossGrow && now.Sub(s.stableSince) >= simulcastGrowAfter
		if healthy {
			if next := s.nextAbove(s.rankOf(wanted)); next != "" {
				wanted = next
			}
		}
		if !s.lastDrop.IsZero() && now.Sub(s.lastDrop) < simulcastGrowAfter {
			wanted = s.lowerThan(s.rankOf(wanted))
		}
		return s.capBlocked(wanted, now)
	}
	budget := uint64(s.remb) * simulcastREMBRatio / 10
	candidate := ""
	for _, limit := range s.limits {
		if s.available[limit.rid] && uint64(limit.bitrate) <= budget {
			candidate = limit.rid
		}
	}
	if candidate == "" {
		candidate = s.lowestLayer()
	}
	if !s.lastDrop.IsZero() && now.Sub(s.lastDrop) < simulcastGrowAfter {
		if s.rankOf(candidate) >= s.rankOf(s.current) {
			candidate = s.lowerThan(s.rankOf(s.current))
		}
	}
	return s.capBlocked(candidate, now)
}

// capBlocked keeps a failed layer and everything above it out of the choice
// until its cooldown expires.
func (s *layerSelector) capBlocked(wanted string, now time.Time) string {
	if s.blockedAbove == "" || !now.Before(s.blockedUntil) || wanted == "" {
		return wanted
	}
	if s.rankOf(wanted) < s.rankOf(s.blockedAbove) {
		return wanted
	}
	for index := s.rankOf(s.blockedAbove) - 1; index >= 0; index-- {
		if s.available[s.limits[index].rid] {
			return s.limits[index].rid
		}
	}
	return wanted
}

func (s *layerSelector) nextAbove(rank int) string {
	for index := rank + 1; index < len(s.limits); index++ {
		if s.available[s.limits[index].rid] {
			return s.limits[index].rid
		}
	}
	return ""
}

func (s *layerSelector) lowerThan(rank int) string {
	for index := rank - 1; index >= 0; index-- {
		if s.available[s.limits[index].rid] {
			return s.limits[index].rid
		}
	}
	return s.current
}

func (s *layerSelector) lowestLayer() string {
	for _, limit := range s.limits {
		if s.available[limit.rid] {
			return limit.rid
		}
	}
	return ""
}

func (s *layerSelector) fallbackLayer() string {
	// Prefer one step down, then anything below, then the best remaining
	// encoding. This is used when the forwarded encoding itself disappears.
	currentRank := s.rankOf(s.current)
	if currentRank > 0 {
		for index := currentRank - 1; index >= 0; index-- {
			if s.available[s.limits[index].rid] {
				return s.limits[index].rid
			}
		}
	}
	for index := len(s.limits) - 1; index > currentRank; index-- {
		if s.available[s.limits[index].rid] {
			return s.limits[index].rid
		}
	}
	return ""
}

// rtpRewriter gives one subscriber a continuous packet stream across layer
// switches: stable SSRC, sequential sequence numbers and a monotonically
// increasing timestamp that does not jump when the encoder changes.
type rtpRewriter struct {
	outSSRC    uint32
	seq        uint16
	tsOffset   uint32
	lastOutTS  uint32
	started    bool
	switchNext bool
}

func newRTPRewriter(outSSRC uint32) *rtpRewriter {
	// A random-ish initial sequence number is what a browser expects; deriving
	// it from the SSRC keeps the rewriter deterministic in tests.
	return &rtpRewriter{outSSRC: outSSRC, seq: uint16(outSSRC)}
}

// beginLayerSwitch makes the next rewriter call align the output timeline to
// the new layer. It is only called for a key frame of the target layer.
func (r *rtpRewriter) beginLayerSwitch() {
	r.switchNext = true
}

func (r *rtpRewriter) rewrite(packet *rtp.Packet) {
	switch {
	case !r.started:
		r.started = true
		r.tsOffset = 0
	case r.switchNext:
		// Continue exactly one frame after the last output packet. RTP
		// timestamps are modulo 2^32, so wrap-around is expected arithmetic.
		r.tsOffset = r.lastOutTS + simulcastTimestampStep - packet.Timestamp
		r.switchNext = false
	}
	packet.SSRC = r.outSSRC
	packet.SequenceNumber = r.seq
	r.seq++
	packet.Timestamp += r.tsOffset
	r.lastOutTS = packet.Timestamp
}

func randomOutSSRC() uint32 {
	for {
		if value := rand.Uint32(); value != 0 {
			return value
		}
	}
}

// isKeyFrame recognises the start of a decodable frame for the codecs the SFU
// forwards. An unknown codec is treated as always decodable: blocking a layer
// switch forever is worse than a brief artifact on an exotic codec.
func isKeyFrame(packet *rtp.Packet, mimeType string) bool {
	switch {
	case strings.EqualFold(mimeType, webrtc.MimeTypeVP8):
		return vp8KeyFrame(packet.Payload)
	case strings.EqualFold(mimeType, webrtc.MimeTypeVP9):
		return vp9KeyFrame(packet.Payload)
	case strings.EqualFold(mimeType, webrtc.MimeTypeH264):
		return h264KeyFrame(packet.Payload)
	default:
		return true
	}
}

// vp8KeyFrame reads the VP8 payload descriptor (RFC 7741) and the payload
// header's P bit. S=1 starts a frame, PID=0 is the first partition, P=0 is a
// key frame.
func vp8KeyFrame(payload []byte) bool {
	if len(payload) < 2 {
		return false
	}
	descriptor := payload[0]
	if descriptor&0x10 == 0 || descriptor&0x0F != 0 {
		return false
	}
	index := 1
	if descriptor&0x80 != 0 {
		if len(payload) < 2 {
			return false
		}
		extensions := payload[1]
		index = 2
		if extensions&0x80 != 0 { // PictureID: one or two bytes
			if index >= len(payload) {
				return false
			}
			index++
			if payload[index-1]&0x80 != 0 {
				index++
			}
		}
		if extensions&0x40 != 0 { // TL0PICIDX
			index++
		}
		if extensions&0x30 != 0 { // TID / KEYIDX
			index++
		}
	}
	if index >= len(payload) {
		return false
	}
	return payload[index]&0x01 == 0
}

// vp9KeyFrame reads the VP9 payload descriptor. B=1 begins a frame and P=0
// marks it as intra-coded.
func vp9KeyFrame(payload []byte) bool {
	if len(payload) < 1 {
		return false
	}
	descriptor := payload[0]
	return descriptor&0x08 != 0 && descriptor&0x40 == 0
}

// h264KeyFrame scans Annex-B or single-NAL payloads for an IDR slice (type 5).
func h264KeyFrame(payload []byte) bool {
	for index := 0; index+3 < len(payload); index++ {
		if payload[index] != 0 || payload[index+1] != 0 {
			continue
		}
		start := 0
		if payload[index+2] == 1 {
			start = index + 3
		} else if payload[index+2] == 0 && payload[index+3] == 1 {
			start = index + 4
		}
		if start > 0 && start < len(payload) && payload[start]&0x1F == 5 {
			return true
		}
	}
	return false
}
