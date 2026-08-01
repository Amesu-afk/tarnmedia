package config

import (
	"strings"
	"testing"

	"github.com/tarnveil/tarnmedia/internal/auth"
)

func TestOriginAllowlistIsExact(t *testing.T) {
	cfg := Config{AllowedOrigins: map[string]struct{}{"https://app.example": {}}}
	if !cfg.OriginAllowed("https://app.example") {
		t.Fatal("configured origin should be allowed")
	}
	if cfg.OriginAllowed("https://app.example.evil.test") {
		t.Fatal("lookalike origin must be rejected")
	}
}

func TestLoadDefaultsAreDeploymentNeutral(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Issuer != auth.DefaultIssuer {
		t.Fatalf("unexpected default issuer %q", cfg.Issuer)
	}
	for origin := range cfg.AllowedOrigins {
		if !strings.Contains(origin, "localhost") && !strings.Contains(origin, "127.0.0.1") {
			t.Fatalf("default origin allowlist must stay local-only, got %q", origin)
		}
	}
}

func TestLoadReadsCustomIssuer(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")
	t.Setenv("TARNMEDIA_ISSUER", "my-app")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Issuer != "my-app" {
		t.Fatalf("unexpected issuer %q", cfg.Issuer)
	}
}
