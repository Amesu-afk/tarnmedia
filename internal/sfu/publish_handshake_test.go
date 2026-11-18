package sfu

import (
	"context"
	"encoding/json"
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

type signalCollector struct {
	events chan signalMessage
	errs   chan error
}

func collectSignals(conn *websocket.Conn) *signalCollector {
	collector := &signalCollector{
		events: make(chan signalMessage, 64),
		errs:   make(chan error, 1),
	}
	go func() {
		for {
			_, data, err := conn.ReadMessage()
			if err != nil {
				collector.errs <- err
				return
			}
			message := signalMessage{}
			if err := json.Unmarshal(data, &message); err != nil {
				collector.errs <- err
				return
			}
			collector.events <- message
		}
	}()
	return collector
}

func (c *signalCollector) wait(t *testing.T, event string) signalMessage {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		select {
		case message := <-c.events:
			if message.Event == event {
				return message
			}
		case err := <-c.errs:
			t.Fatalf("signaling read failed while waiting for %q: %v", event, err)
		case <-deadline:
			t.Fatalf("timed out waiting for signaling event %q", event)
		}
	}
}

func (c *signalCollector) expectNoOffer(t *testing.T, window time.Duration) {
	t.Helper()
	timer := time.After(window)
	for {
		select {
		case message := <-c.events:
			if message.Event == "offer" {
				t.Fatal("the SFU sent a downstream offer before the publish handshake completed")
			}
		case err := <-c.errs:
			t.Fatalf("signaling read failed: %v", err)
		case <-timer:
			return
		}
	}
}

func newPublishTestServer(t *testing.T, simulcast bool) (*Server, string, string) {
	t.Helper()
	const jwtSecret = "publish-test-jwt-secret-long-enough"
	validator := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(validator.Close)

	mediaServer, err := New(config.Config{
		JWTSecret:       jwtSecret,
		ControlSecret:   "publish-test-control-secret-long-enough",
		Issuer:          auth.DefaultIssuer,
		AuthURL:         validator.URL,
		AllowedOrigins:  map[string]struct{}{"https://tarnveil.ru": {}},
		MaxPeersPerRoom: 10,
		Simulcast:       simulcast,
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
	return mediaServer, "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/v1/ws", jwtSecret
}

func publishTestToken(t *testing.T, jwtSecret, id string) string {
	t.Helper()
	claims := auth.Claims{
		Room: "publish-room", ParticipantID: id, UserID: "user-" + id,
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
		t.Fatal(err)
	}
	return token
}

func dialPublishPeer(t *testing.T, wsURL, jwtSecret, id string, publish, simulcast bool) (*websocket.Conn, *signalCollector) {
	t.Helper()
	header := http.Header{"Origin": []string{"https://tarnveil.ru"}}
	conn, _, err := websocket.DefaultDialer.DialContext(context.Background(), wsURL, header)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	collector := collectSignals(conn)
	payload := map[string]any{"token": publishTestToken(t, jwtSecret, id)}
	if publish {
		payload["publish"] = true
	}
	if simulcast {
		payload["simulcast"] = true
	}
	if err := conn.WriteJSON(map[string]any{"event": "authenticate", "data": payload}); err != nil {
		t.Fatal(err)
	}
	return conn, collector
}

func TestPublishHandshakeAnswersAfterGatheringAndDefersDownstreamOffers(t *testing.T) {
	_, wsURL, jwtSecret := newPublishTestServer(t, true)
	conn, signals := dialPublishPeer(t, wsURL, jwtSecret, "publisher", true, true)

	authenticated := signals.wait(t, "authenticated")
	var hello struct {
		Publish   bool `json:"publish"`
		Simulcast bool `json:"simulcast"`
	}
	if err := json.Unmarshal(authenticated.Data, &hello); err != nil {
		t.Fatal(err)
	}
	if !hello.Publish || !hello.Simulcast {
		t.Fatalf("authenticated capabilities = %+v, want publish and simulcast", hello)
	}
	// The old protocol offered recvonly slots immediately. A publisher must not
	// get that offer: it is the offerer for its media.
	signals.expectNoOffer(t, 200*time.Millisecond)

	pc, err := webrtc.NewAPI().NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	// Pion expresses simulcast on the send side through one track per RID on a
	// single sender. Chrome does the same with sendEncodings; both put
	// a=simulcast:send and a=rid:<rid> send in the offer.
	videoTrack, err := webrtc.NewTrackLocalStaticRTP(
		webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "camera", "publisher",
		webrtc.WithRTPStreamID("q"),
	)
	if err != nil {
		t.Fatal(err)
	}
	video, err := pc.AddTransceiverFromTrack(videoTrack, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, rid := range []string{"h", "f"} {
		extra, err := webrtc.NewTrackLocalStaticRTP(
			webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypeVP8}, "camera", "publisher",
			webrtc.WithRTPStreamID(rid),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := video.Sender().AddEncoding(extra); err != nil {
			t.Fatal(err)
		}
	}
	audio, err := pc.AddTransceiverFromKind(webrtc.RTPCodecTypeAudio, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	})
	if err != nil {
		t.Fatal(err)
	}
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.SetLocalDescription(offer); err != nil {
		t.Fatal(err)
	}
	if video.Mid() == "" || audio.Mid() == "" {
		t.Fatal("offer assigned no mids to the publish slots")
	}
	payload := publishOfferData{
		SDP: *pc.LocalDescription(),
		MidToSource: map[string]string{
			video.Mid(): "camera",
			audio.Mid(): "microphone",
		},
	}
	if err := conn.WriteJSON(map[string]any{"event": "publish_offer", "data": payload}); err != nil {
		t.Fatal(err)
	}

	answerMessage := signals.wait(t, "publish_answer")
	answer := webrtc.SessionDescription{}
	if err := json.Unmarshal(answerMessage.Data, &answer); err != nil {
		t.Fatal(err)
	}
	if answer.Type != webrtc.SDPTypeAnswer || answer.SDP == "" {
		t.Fatalf("publish answer is not a WebRTC answer: %+v", answer)
	}
	// Gathering must be finished before the answer is sent: the client gets a
	// complete candidate set in one SDP instead of racing a trickled answer.
	if !strings.Contains(answer.SDP, "a=candidate:") {
		t.Fatal("publish answer was sent before ICE gathering completed")
	}
	if !strings.Contains(answer.SDP, "a=simulcast:recv") || !strings.Contains(answer.SDP, "a=rid:q recv") {
		t.Fatalf("publish answer does not accept the RID layers:\n%s", answer.SDP)
	}
	// Without the RID header extension Pion cannot attribute an incoming RTP
	// packet to a layer and OnTrack arrives with an empty RID.
	if !strings.Contains(answer.SDP, "sdes:rtp-stream-id") {
		t.Fatalf("publish answer does not negotiate the RID header extension:\n%s", answer.SDP)
	}
	if err := pc.SetRemoteDescription(answer); err != nil {
		t.Fatal(err)
	}
	// There are no other publishers in the room, so no downstream offer follows.
	signals.expectNoOffer(t, 200*time.Millisecond)
}

func TestSimulcastOffKeepsTheOriginalServerOffer(t *testing.T) {
	_, wsURL, jwtSecret := newPublishTestServer(t, false)
	_, signals := dialPublishPeer(t, wsURL, jwtSecret, "legacy", true, true)

	authenticated := signals.wait(t, "authenticated")
	var hello struct {
		Publish   bool `json:"publish"`
		Simulcast bool `json:"simulcast"`
	}
	if err := json.Unmarshal(authenticated.Data, &hello); err != nil {
		t.Fatal(err)
	}
	if hello.Publish || hello.Simulcast {
		t.Fatalf("authenticated capabilities = %+v, want the legacy path", hello)
	}
	// With the feature off a newer client is treated exactly like an old one.
	offerMessage := signals.wait(t, "offer")
	offer := webrtc.SessionDescription{}
	if err := json.Unmarshal(offerMessage.Data, &offer); err != nil {
		t.Fatal(err)
	}
	if offer.Type != webrtc.SDPTypeOffer || !strings.Contains(offer.SDP, "a=recvonly") {
		t.Fatalf("the legacy server offer is missing recvonly slots:\n%s", offer.SDP)
	}
}
