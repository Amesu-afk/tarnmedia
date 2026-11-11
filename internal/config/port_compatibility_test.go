package config

import "testing"

// These destination ranges are the unconditional Discord/STUN profiles in all
// 22 Flowseal strategies at commit 2401b7a13837139b0265632464ba563dddbf80d8.
// They are shared STUN traffic filters, not a Discord hostname allowlist.
func TestDefaultMediaPortsAvoidDiscordSTUNProfiles(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("TARNMEDIA_JWT_SECRET", "test-jwt-secret-that-is-long-enough")
	t.Setenv("TARNMEDIA_CONTROL_SECRET", "test-control-secret-that-is-long-enough")
	t.Setenv("TARNMEDIA_UDP_MIN", "")
	t.Setenv("TARNMEDIA_UDP_MAX", "")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, blocked := range [][2]uint16{{19294, 19344}, {50000, 50100}} {
		if cfg.UDPMin <= blocked[1] && cfg.UDPMax >= blocked[0] {
			t.Fatalf("default ICE/STUN range %d-%d overlaps Discord profile %d-%d", cfg.UDPMin, cfg.UDPMax, blocked[0], blocked[1])
		}
	}
	if int(cfg.UDPMax)-int(cfg.UDPMin)+1 < 101 {
		t.Fatal("default media range must retain at least the existing 101-port capacity")
	}
}
