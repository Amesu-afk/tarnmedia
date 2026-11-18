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

func TestSimulcastDefaultsAreOnWithSharedLayerThresholds(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Simulcast {
		t.Fatal("simulcast must be on by default")
	}
	want := []SimulcastLayer{{RID: "q", Bitrate: 150_000}, {RID: "h", Bitrate: 400_000}, {RID: "f", Bitrate: 1_200_000}}
	if len(cfg.SimulcastLayers) != len(want) {
		t.Fatalf("layers = %+v, want %+v", cfg.SimulcastLayers, want)
	}
	for index := range want {
		if cfg.SimulcastLayers[index] != want[index] {
			t.Fatalf("layers = %+v, want %+v", cfg.SimulcastLayers, want)
		}
	}
}

func TestSimulcastLayersCanBeConfiguredAndAreOrderedByBitrate(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")
	t.Setenv("TARNMEDIA_SIMULCAST_LAYERS", " f = 1300000 , q=120000 , h=450000 ")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	got := []string{cfg.SimulcastLayers[0].RID, cfg.SimulcastLayers[1].RID, cfg.SimulcastLayers[2].RID}
	want := []string{"q", "h", "f"}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("layer order = %v, want %v", got, want)
		}
	}
	if cfg.SimulcastLayers[2].Bitrate != 1_300_000 {
		t.Fatalf("f bitrate = %d, want 1300000", cfg.SimulcastLayers[2].Bitrate)
	}
}

func TestSimulcastOffKeepsTheLegacyServerPath(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")
	t.Setenv("TARNMEDIA_SIMULCAST", "off")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Simulcast {
		t.Fatal("TARNMEDIA_SIMULCAST=off must disable the feature")
	}
}

func TestSimulcastSettingsAreValidated(t *testing.T) {
	t.Setenv("TARNMEDIA_JWT_SECRET", "a-test-secret-that-is-long-enough-here")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "another-test-secret-that-is-long-enough")

	for _, value := range []string{"maybe", "q", "q=0", "q=abc", "q=100,h=200,q=300", "q=100,,h=200", "q=100,h=200000000"} {
		t.Setenv("TARNMEDIA_SIMULCAST_LAYERS", value)
		if _, err := Load(); err == nil {
			t.Fatalf("TARNMEDIA_SIMULCAST_LAYERS=%q must be rejected", value)
		}
	}
	t.Setenv("TARNMEDIA_SIMULCAST_LAYERS", "q=150000,h=400000,f=1200000")
	t.Setenv("TARNMEDIA_SIMULCAST", "sometimes")
	if _, err := Load(); err == nil {
		t.Fatal("an unknown TARNMEDIA_SIMULCAST value must be rejected")
	}
}
