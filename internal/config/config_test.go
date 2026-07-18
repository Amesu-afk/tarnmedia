package config

import "testing"

func TestOriginAllowlistIsExact(t *testing.T) {
	cfg := Config{AllowedOrigins: map[string]struct{}{"https://tarnveil.ru": {}}}
	if !cfg.OriginAllowed("https://tarnveil.ru") {
		t.Fatal("production origin should be allowed")
	}
	if cfg.OriginAllowed("https://tarnveil.ru.evil.example") {
		t.Fatal("lookalike origin must be rejected")
	}
}
