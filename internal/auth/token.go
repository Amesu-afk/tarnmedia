package auth

import (
	"errors"
	"fmt"
	"strings"

	"github.com/golang-jwt/jwt/v5"
)

const (
	Audience = "tarnmedia"
	Issuer   = "tarnveil-server"
)

type Claims struct {
	Room           string `json:"room"`
	ParticipantID  string `json:"participantId"`
	UserID         string `json:"userId"`
	SessionVersion int    `json:"sessionVersion"`
	Username       string `json:"username"`
	DisplayName    string `json:"displayName"`
	AvatarURL      string `json:"avatarUrl,omitempty"`
	IssuedAtMS     int64  `json:"issuedAtMs"`
	jwt.RegisteredClaims
}

func Parse(raw, secret string) (Claims, error) {
	if len(raw) > 8192 {
		return Claims{}, errors.New("token is too large")
	}
	claims := Claims{}
	token, err := jwt.ParseWithClaims(
		raw,
		&claims,
		func(token *jwt.Token) (any, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, fmt.Errorf("unexpected signing method %s", token.Method.Alg())
			}
			return []byte(secret), nil
		},
		jwt.WithAudience(Audience),
		jwt.WithIssuer(Issuer),
		jwt.WithExpirationRequired(),
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
	)
	if err != nil || !token.Valid {
		return Claims{}, errors.New("invalid or expired media token")
	}
	if claims.Subject == "" || claims.UserID != claims.Subject {
		return Claims{}, errors.New("invalid media token subject")
	}
	if claims.IssuedAt == nil {
		return Claims{}, errors.New("media token has no issue time")
	}
	if claims.IssuedAtMS <= 0 {
		return Claims{}, errors.New("media token has no precise issue time")
	}
	if claims.SessionVersion < 0 {
		return Claims{}, errors.New("media token has an invalid session version")
	}
	if !validIdentifier(claims.Room, 180) || !validIdentifier(claims.ParticipantID, 180) {
		return Claims{}, errors.New("invalid room or participant identifier")
	}
	if strings.TrimSpace(claims.Username) == "" {
		return Claims{}, errors.New("media token has no username")
	}
	return claims, nil
}

func validIdentifier(value string, max int) bool {
	if value == "" || len(value) > max {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
