package sfu

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

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
