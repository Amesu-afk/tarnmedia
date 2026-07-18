package auth

import (
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

func TestParseAcceptsScopedToken(t *testing.T) {
	secret := "test-secret-that-is-long-enough-for-hs256"
	claims := Claims{
		Room: "voice-room", ParticipantID: "user-1__device", UserID: "user-1", Username: "alice",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user-1", Issuer: Issuer, Audience: jwt.ClaimStrings{Audience},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	}
	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := Parse(raw, secret)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Room != claims.Room || parsed.ParticipantID != claims.ParticipantID {
		t.Fatalf("unexpected claims: %#v", parsed)
	}
}

func TestParseRejectsWrongRoomTokenAudience(t *testing.T) {
	secret := "test-secret-that-is-long-enough-for-hs256"
	claims := Claims{
		Room: "voice-room", ParticipantID: "peer", UserID: "user-1", Username: "alice",
		RegisteredClaims: jwt.RegisteredClaims{
			Subject: "user-1", Issuer: Issuer, Audience: jwt.ClaimStrings{"another-service"},
			ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Minute)),
		},
	}
	raw, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Parse(raw, secret); err == nil {
		t.Fatal("expected token with wrong audience to be rejected")
	}
}
