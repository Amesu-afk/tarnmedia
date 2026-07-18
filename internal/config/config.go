package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	Addr            string
	JWTSecret       string
	AllowedOrigins  map[string]struct{}
	PublicIP        string
	UDPMin          uint16
	UDPMax          uint16
	ICEURLs         []string
	ICEUsername     string
	ICECredential   string
	MaxPeersPerRoom int
}

func Load() (Config, error) {
	cfg := Config{
		Addr:            env("TARNMEDIA_ADDR", ":8088"),
		JWTSecret:       strings.TrimSpace(os.Getenv("TARNMEDIA_JWT_SECRET")),
		PublicIP:        strings.TrimSpace(os.Getenv("TARNMEDIA_PUBLIC_IP")),
		ICEURLs:         splitCSV(os.Getenv("TARNMEDIA_ICE_URLS")),
		ICEUsername:     os.Getenv("TARNMEDIA_ICE_USERNAME"),
		ICECredential:   os.Getenv("TARNMEDIA_ICE_CREDENTIAL"),
		MaxPeersPerRoom: envInt("TARNMEDIA_MAX_PEERS_PER_ROOM", 25),
		AllowedOrigins:  make(map[string]struct{}),
	}

	if cfg.JWTSecret == "" {
		return Config{}, fmt.Errorf("TARNMEDIA_JWT_SECRET is required")
	}
	if len(cfg.JWTSecret) < 32 {
		return Config{}, fmt.Errorf("TARNMEDIA_JWT_SECRET must be at least 32 characters")
	}
	if cfg.MaxPeersPerRoom < 2 || cfg.MaxPeersPerRoom > 100 {
		return Config{}, fmt.Errorf("TARNMEDIA_MAX_PEERS_PER_ROOM must be between 2 and 100")
	}

	min, err := envUint16("TARNMEDIA_UDP_MIN", 50000)
	if err != nil {
		return Config{}, err
	}
	max, err := envUint16("TARNMEDIA_UDP_MAX", 50100)
	if err != nil {
		return Config{}, err
	}
	if min > max {
		return Config{}, fmt.Errorf("TARNMEDIA_UDP_MIN must not exceed TARNMEDIA_UDP_MAX")
	}
	cfg.UDPMin, cfg.UDPMax = min, max

	for _, origin := range splitCSV(env("TARNMEDIA_ALLOWED_ORIGINS", "https://tarnveil.ru,https://www.tarnveil.ru,http://localhost:5173,http://127.0.0.1:5173,http://tauri.localhost")) {
		u, err := url.Parse(origin)
		if err != nil || u.Scheme == "" || u.Host == "" || u.Path != "" {
			return Config{}, fmt.Errorf("invalid origin in TARNMEDIA_ALLOWED_ORIGINS: %q", origin)
		}
		cfg.AllowedOrigins[u.Scheme+"://"+u.Host] = struct{}{}
	}
	return cfg, nil
}

func (c Config) OriginAllowed(raw string) bool {
	if raw == "" {
		return true
	}
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	_, ok := c.AllowedOrigins[u.Scheme+"://"+u.Host]
	return ok
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func envInt(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return parsed
}

func envUint16(name string, fallback uint16) (uint16, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 16)
	if err != nil || parsed == 0 {
		return 0, fmt.Errorf("%s must be a valid UDP port", name)
	}
	return uint16(parsed), nil
}
