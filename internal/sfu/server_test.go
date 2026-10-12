package sfu

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/pion/rtcp"

	"github.com/tarnveil/tarnmedia/internal/auth"
	"github.com/tarnveil/tarnmedia/internal/config"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	server, err := New(config.Config{
		JWTSecret:       "jwt-secret-that-is-long-enough-for-tests",
		ControlSecret:   "control-secret-that-is-long-enough-for-tests",
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		UDPMin:          55000,
		UDPMax:          55010,
		MaxPeersPerRoom: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(server.Close)
	return server
}

func TestControlEndpointRequiresBearerSecret(t *testing.T) {
	server := testServer(t)
	request := httptest.NewRequest(http.MethodPost, "/v1/control", bytes.NewBufferString(`{"action":"closeRoom","room":"room-1","revokedBeforeMs":1}`))
	response := httptest.NewRecorder()
	server.ControlHandler().ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", response.Code)
	}
}

func TestCapabilitiesRequireSecretAndReturnActualRoomLimit(t *testing.T) {
	server := testServer(t)
	for _, authorized := range []bool{false, true} {
		request := httptest.NewRequest(http.MethodGet, "/v1/capabilities", nil)
		if authorized {
			request.Header.Set("Authorization", "Bearer control-secret-that-is-long-enough-for-tests")
		}
		response := httptest.NewRecorder()
		server.ControlHandler().ServeHTTP(response, request)
		if !authorized {
			if response.Code != http.StatusUnauthorized {
				t.Fatalf("expected 401, got %d", response.Code)
			}
			continue
		}
		var result struct {
			MaxPeersPerRoom int `json:"maxPeersPerRoom"`
		}
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || result.MaxPeersPerRoom != 10 {
			t.Fatalf("incorrect capabilities: %d %s", response.Code, response.Body.String())
		}
	}
}

func TestCloseRoomRevokesAlreadyIssuedTokens(t *testing.T) {
	server := testServer(t)
	issuedAt := time.Now().Add(-time.Second)
	claims := auth.Claims{
		Room: "room-1", UserID: "user-1", IssuedAtMS: issuedAt.UnixMilli(),
		RegisteredClaims: jwt.RegisteredClaims{IssuedAt: jwt.NewNumericDate(issuedAt)},
	}
	body := []byte(fmt.Sprintf(`{"action":"closeRoom","room":"room-1","revokedBeforeMs":%d}`, time.Now().UnixMilli()))
	request := httptest.NewRequest(http.MethodPost, "/v1/control", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer control-secret-that-is-long-enough-for-tests")
	response := httptest.NewRecorder()
	server.ControlHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if !server.tokenRevoked(claims) {
		t.Fatal("token issued before room closure must be revoked")
	}
}

func TestRevokeUserRevokesTokensAcrossRooms(t *testing.T) {
	server := testServer(t)
	issuedAt := time.Now().Add(-time.Second)
	claims := auth.Claims{
		Room: "room-2", UserID: "user-1", SessionVersion: 3, IssuedAtMS: issuedAt.UnixMilli(),
		RegisteredClaims: jwt.RegisteredClaims{IssuedAt: jwt.NewNumericDate(issuedAt)},
	}
	body := []byte(fmt.Sprintf(`{"action":"revokeUser","userId":"user-1","revokedBeforeMs":%d,"sessionVersion":4}`, time.Now().UnixMilli()))
	request := httptest.NewRequest(http.MethodPost, "/v1/control", bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer control-secret-that-is-long-enough-for-tests")
	response := httptest.NewRecorder()
	server.ControlHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", response.Code, response.Body.String())
	}
	if !server.tokenRevoked(claims) {
		t.Fatal("global user revocation must reject a token from any room")
	}
	freshClaims := claims
	freshClaims.SessionVersion = 4
	if server.tokenRevoked(freshClaims) {
		t.Fatal("current session version must remain valid even if it was issued in the same millisecond as revocation")
	}
}

func TestValidateSessionRequiresCurrentAPISession(t *testing.T) {
	server := testServer(t)
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.Header.Get("Authorization") != "Bearer control-secret-that-is-long-enough-for-tests" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer validator.Close()
	server.cfg.AuthURL = validator.URL
	server.authHTTP = validator.Client()

	if err := server.validateSession(auth.Claims{UserID: "user-1", SessionVersion: 2}); err != nil {
		t.Fatalf("expected current API session to be accepted: %v", err)
	}
}

func TestValidateSessionFailsClosed(t *testing.T) {
	server := testServer(t)
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer validator.Close()
	server.cfg.AuthURL = validator.URL
	server.authHTTP = validator.Client()

	if err := server.validateSession(auth.Claims{UserID: "user-1", SessionVersion: 2}); err == nil {
		t.Fatal("revoked API session must be rejected")
	}
}

func TestMetricsAreAvailableOnControlHandler(t *testing.T) {
	server := testServer(t)
	server.SetReady(true)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.ControlHandler().ServeHTTP(response, request)
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.Code != http.StatusOK || !strings.Contains(string(body), "tarnmedia_ready 1") {
		t.Fatalf("unexpected metrics response: %d %s", response.Code, body)
	}
}

func TestStateSignalSetsMicTransmitting(t *testing.T) {
	tests := []struct {
		name             string
		data             string
		wantTransmitting bool
	}{
		{
			name:             "new client starts transmitting",
			data:             `{"micMuted":false,"micTransmitting":true}`,
			wantTransmitting: true,
		},
		{
			name:             "new client reports PTT or VAD closed",
			data:             `{"micMuted":false,"micTransmitting":false}`,
			wantTransmitting: false,
		},
		{
			name:             "legacy client omits transmission field",
			data:             `{"micMuted":false}`,
			wantTransmitting: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			peer := &peer{room: &room{}}
			if err := peer.handleSignal(signalMessage{Event: "state", Data: json.RawMessage(test.data)}); err != nil {
				t.Fatal(err)
			}
			if got := peer.allowsPacketForwarding("microphone"); got != test.wantTransmitting {
				t.Fatalf("microphone forwarding = %t, want %t", got, test.wantTransmitting)
			}
		})
	}
}

func TestKeyFrameRequestsCollapseWithinTheInterval(t *testing.T) {
	gate := &keyFrameGate{}
	start := time.Now()
	if !gate.allow(start) {
		t.Fatal("the first request must reach the publisher")
	}
	// A full room asks all at once: one key frame answers everyone.
	if gate.allow(start.Add(keyFrameMinInterval - time.Millisecond)) {
		t.Fatal("a second request inside the interval must be collapsed into the first")
	}
	if !gate.allow(start.Add(keyFrameMinInterval)) {
		t.Fatal("a request after the interval must reach the publisher again")
	}
}

func TestSubscriberRTCPIsRecognisedAsKeyFrameRequest(t *testing.T) {
	tests := []struct {
		name    string
		packets []rtcp.Packet
		want    bool
	}{
		{
			name:    "picture loss indication",
			packets: []rtcp.Packet{&rtcp.PictureLossIndication{MediaSSRC: 4242}},
			want:    true,
		},
		{
			name:    "full intra request",
			packets: []rtcp.Packet{&rtcp.FullIntraRequest{MediaSSRC: 4242, FIR: []rtcp.FIREntry{{SSRC: 4242, SequenceNumber: 1}}}},
			want:    true,
		},
		{
			name:    "receiver report is handled by the interceptors, not by us",
			packets: []rtcp.Packet{&rtcp.ReceiverReport{SSRC: 4242}},
			want:    false,
		},
		{
			name:    "a request bundled behind a report is still found",
			packets: []rtcp.Packet{&rtcp.ReceiverReport{SSRC: 4242}, &rtcp.PictureLossIndication{MediaSSRC: 4242}},
			want:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Go through the wire format: this is what arrives from a subscriber.
			raw, err := rtcp.Marshal(test.packets)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := rtcp.Unmarshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			if got := requestsKeyFrame(decoded); got != test.want {
				t.Fatalf("requestsKeyFrame = %t, want %t", got, test.want)
			}
		})
	}
}

// testUplink builds a forwarded track whose publisher has a real PeerConnection, so
// requestKeyFrame runs the path it runs in production.
func testUplink(t *testing.T, server *Server, source string, video bool) localTrack {
	t.Helper()
	pc, err := server.api.NewPeerConnection(server.pcConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = pc.Close() })
	uplink := localTrack{
		ownerID: "publisher", source: source, ssrc: 4242,
		owner: &peer{id: "publisher", pc: pc},
	}
	if video {
		uplink.keyFrames = &keyFrameGate{}
	}
	return uplink
}

func TestNewSubscribersShareOneKeyFrame(t *testing.T) {
	server := testServer(t)
	camera := testUplink(t, server, "camera", true)
	// Three subscribers finish negotiating at once: one key frame answers all of them.
	camera.requestKeyFrame(&server.metrics)
	camera.requestKeyFrame(&server.metrics)
	camera.requestKeyFrame(&server.metrics)
	if got := server.metrics.keyFramesRequested.Load(); got != 1 {
		t.Fatalf("key frame requests reaching the publisher = %d, want 1", got)
	}
	if got := server.metrics.keyFramesCollapsed.Load(); got != 2 {
		t.Fatalf("collapsed requests = %d, want 2", got)
	}
}

func TestAudioUplinkNeverAsksForAKeyFrame(t *testing.T) {
	server := testServer(t)
	// forward() leaves keyFrames nil for audio, so a stray request must do nothing.
	microphone := testUplink(t, server, "microphone", false)
	microphone.requestKeyFrame(&server.metrics)
	if server.metrics.keyFramesRequested.Load() != 0 || server.metrics.keyFramesCollapsed.Load() != 0 {
		t.Fatal("an audio uplink must not produce key frame traffic")
	}
}

func TestKeyFrameCountersAreExposed(t *testing.T) {
	server := testServer(t)
	server.metrics.keyFramesRequested.Add(2)
	server.metrics.keyFramesCollapsed.Add(5)
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()
	server.ControlHandler().ServeHTTP(response, request)
	body := response.Body.String()
	if !strings.Contains(body, "tarnmedia_key_frames_requested_total 2") {
		t.Fatalf("key frame requests missing from metrics: %s", body)
	}
	if !strings.Contains(body, "tarnmedia_key_frames_collapsed_total 5") {
		t.Fatalf("collapsed key frame requests missing from metrics: %s", body)
	}
}

func TestOnlyNewlySubscribedTracksAreQueuedForAKeyFrame(t *testing.T) {
	server := testServer(t)
	subscriber := &peer{id: "subscriber"}
	camera := testUplink(t, server, "camera", true)
	subscriber.pendingKeyFrames = append(subscriber.pendingKeyFrames, camera)

	// This is what the answer branch does: take the queue and clear it.
	queued := subscriber.pendingKeyFrames
	subscriber.pendingKeyFrames = nil
	for _, candidate := range queued {
		candidate.requestKeyFrame(&server.metrics)
	}
	if got := server.metrics.keyFramesRequested.Load(); got != 1 {
		t.Fatalf("a new subscription must ask for one key frame, got %d", got)
	}

	// Renegotiation also happens on every mute toggle. That answer carries no new
	// subscription, so it must cost the room nothing.
	if subscriber.pendingKeyFrames != nil {
		t.Fatal("the queue must be empty once the answer consumed it")
	}
	for _, candidate := range subscriber.pendingKeyFrames {
		candidate.requestKeyFrame(&server.metrics)
	}
	if got := server.metrics.keyFramesRequested.Load(); got != 1 {
		t.Fatalf("a mute toggle must not ask for key frames, requests = %d", got)
	}
}

func TestMicrophonePacketsRequireUnmutedTransmittingState(t *testing.T) {
	peer := &peer{}
	if peer.allowsPacketForwarding("microphone") {
		t.Fatal("microphone packets must be blocked until the client reports transmission")
	}

	peer.state.MicTransmitting = true
	if !peer.allowsPacketForwarding("microphone") {
		t.Fatal("an unmuted, transmitting microphone must be forwarded")
	}

	peer.state.MicMuted = true
	if peer.allowsPacketForwarding("microphone") {
		t.Fatal("a muted microphone must be blocked before RTP forwarding")
	}
	if !peer.allowsPacketForwarding("camera") || !peer.allowsPacketForwarding("screen") {
		t.Fatal("mute must not block camera or screen packets")
	}

	peer.state.MicMuted = false
	if !peer.allowsPacketForwarding("microphone") {
		t.Fatal("microphone packets must resume immediately after unmute while transmitting")
	}

	peer.state.MicTransmitting = false
	if peer.allowsPacketForwarding("microphone") {
		t.Fatal("microphone packets must stop when PTT or VAD closes transmission")
	}
}

func TestScreenAudioFollowsTheScreenAndIgnoresMute(t *testing.T) {
	peer := &peer{}
	if peer.sourceActive("screenAudio") {
		t.Fatal("screen audio must not be forwarded while nothing is being shared")
	}

	peer.state.ScreenOn = true
	if !peer.sourceActive("screenAudio") {
		t.Fatal("screen audio must follow the screen it belongs to")
	}

	// Muting the microphone silences the voice, not the shared sound: they are
	// separate slots precisely so that one can be cut without the other.
	peer.state.MicMuted = true
	if !peer.allowsPacketForwarding("screenAudio") {
		t.Fatal("a muted microphone must not silence the shared screen's sound")
	}

	peer.state.ScreenOn = false
	if peer.sourceActive("screenAudio") {
		t.Fatal("stopping the share must stop its sound with it")
	}
}
