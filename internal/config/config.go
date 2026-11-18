package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/tarnveil/tarnmedia/internal/auth"
)

// SimulcastLayer is one RID encoding a publisher may send. Bitrate is the
// budget the layer needs on a subscriber's link; the layer selector uses it to
// choose the highest layer that fits.
type SimulcastLayer struct {
	RID     string `json:"rid"`
	Bitrate uint32 `json:"bitrate"`
}

type Config struct {
	Addr           string
	ControlAddr    string
	ControlSecret  string
	AuthURL        string
	JWTSecret      string
	Issuer         string
	AllowedOrigins map[string]struct{}
	PublicIP       string
	UDPMin         uint16
	UDPMax         uint16
	// Один UDP-порт, который слушает ICE-мультиплексор: все PeerConnection
	// делят его, поэтому потолок в ~100 пиров из диапазона порогов снят.
	UDPPort uint16
	// Simulcast включает приём RID-слоёв и выбор слоя. При выключенном флаге
	// сервер ведёт себя как раньше: один слой и SFU-offer с recvonly-слотами.
	Simulcast bool
	// SimulcastLayers — пороги слоёв по возрастанию битрейта. Общие для сервера
	// и клиента, чтобы выбор слоя не разъезжался с тем, что реально отправляется.
	SimulcastLayers []SimulcastLayer
	ICEURLs         []string
	ICEUsername     string
	ICECredential   string
	MaxPeersPerRoom int
}

func loadDotEnv() {
	paths := []string{".env", "../.env"}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			parts := strings.SplitN(line, "=", 2)
			if len(parts) == 2 {
				k := strings.TrimSpace(parts[0])
				v := strings.TrimSpace(parts[1])
				v = strings.Trim(v, `"'`)
				if os.Getenv(k) == "" {
					os.Setenv(k, v)
				}
			}
		}
	}
}

func Load() (Config, error) {
	loadDotEnv()
	cfg := Config{
		Addr:            env("TARNMEDIA_ADDR", ":8088"),
		ControlAddr:     env("TARNMEDIA_CONTROL_ADDR", "127.0.0.1:8089"),
		ControlSecret:   strings.TrimSpace(os.Getenv("TARNMEDIA_CONTROL_SECRET")),
		AuthURL:         env("TARNMEDIA_AUTH_URL", "http://127.0.0.1:3001/internal/tarnmedia/validate"),
		JWTSecret:       strings.TrimSpace(os.Getenv("TARNMEDIA_JWT_SECRET")),
		Issuer:          env("TARNMEDIA_ISSUER", auth.DefaultIssuer),
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
	if len(cfg.ControlSecret) < 32 {
		return Config{}, fmt.Errorf("TARNMEDIA_CONTROL_SECRET must be at least 32 characters")
	}
	host, _, err := net.SplitHostPort(cfg.ControlAddr)
	if err != nil || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
		return Config{}, fmt.Errorf("TARNMEDIA_CONTROL_ADDR must listen on loopback only")
	}
	authURL, err := url.Parse(cfg.AuthURL)
	if err != nil || authURL.Scheme == "" || authURL.Host == "" || authURL.Fragment != "" || authURL.User != nil {
		return Config{}, fmt.Errorf("TARNMEDIA_AUTH_URL must be a valid URL")
	}
	if authURL.Scheme != "https" && !(authURL.Scheme == "http" && isLoopbackHost(authURL.Hostname())) {
		return Config{}, fmt.Errorf("TARNMEDIA_AUTH_URL must use HTTPS or loopback HTTP")
	}
	if cfg.MaxPeersPerRoom < 2 || cfg.MaxPeersPerRoom > 100 {
		return Config{}, fmt.Errorf("TARNMEDIA_MAX_PEERS_PER_ROOM must be between 2 and 100")
	}

	simulcast, err := envBool("TARNMEDIA_SIMULCAST", true)
	if err != nil {
		return Config{}, err
	}
	cfg.Simulcast = simulcast
	layers, err := parseSimulcastLayers(env("TARNMEDIA_SIMULCAST_LAYERS", "q=150000,h=400000,f=1200000"))
	if err != nil {
		return Config{}, err
	}
	cfg.SimulcastLayers = layers

	// Keep default ICE/STUN traffic outside common Discord interception profiles.
	min, err := envUint16("TARNMEDIA_UDP_MIN", 42000)
	if err != nil {
		return Config{}, err
	}
	max, err := envUint16("TARNMEDIA_UDP_MAX", 42100)
	if err != nil {
		return Config{}, err
	}
	if min > max {
		return Config{}, fmt.Errorf("TARNMEDIA_UDP_MIN must not exceed TARNMEDIA_UDP_MAX")
	}
	cfg.UDPMin, cfg.UDPMax = min, max
	port, err := envUint16("TARNMEDIA_UDP_PORT", min)
	if err != nil {
		return Config{}, err
	}
	if port == 0 {
		return Config{}, fmt.Errorf("TARNMEDIA_UDP_PORT must be between 1 and 65535")
	}
	if port < min || port > max {
		return Config{}, fmt.Errorf("TARNMEDIA_UDP_PORT must be inside TARNMEDIA_UDP_MIN..TARNMEDIA_UDP_MAX")
	}
	cfg.UDPPort = port

	// The default covers local development only. Every deployment is expected to
	// pass its own public origins through TARNMEDIA_ALLOWED_ORIGINS.
	for _, origin := range splitCSV(env("TARNMEDIA_ALLOWED_ORIGINS", "http://localhost:5173,http://127.0.0.1:5173")) {
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

func isLoopbackHost(host string) bool {
	return host == "127.0.0.1" || host == "::1" || host == "localhost"
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

func envBool(name string, fallback bool) (bool, error) {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "":
		return fallback, nil
	case "1", "on", "true", "yes":
		return true, nil
	case "0", "off", "false", "no":
		return false, nil
	default:
		return false, fmt.Errorf("%s must be on or off", name)
	}
}

// parseSimulcastLayers reads "rid=bitrate,rid=bitrate" and returns the layers
// ordered by bitrate. The order is the selector's ordering: q < h < f.
func parseSimulcastLayers(value string) ([]SimulcastLayer, error) {
	parts := strings.Split(value, ",")
	if len(parts) == 0 {
		return nil, fmt.Errorf("TARNMEDIA_SIMULCAST_LAYERS must list at least one layer")
	}
	seen := make(map[string]struct{}, len(parts))
	layers := make([]SimulcastLayer, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("TARNMEDIA_SIMULCAST_LAYERS contains an empty layer")
		}
		rid, rawBitrate, ok := strings.Cut(part, "=")
		rid = strings.TrimSpace(rid)
		rawBitrate = strings.TrimSpace(rawBitrate)
		if !ok || rid == "" || len(rid) > 16 || rawBitrate == "" {
			return nil, fmt.Errorf("invalid simulcast layer %q, want rid=bitrate", part)
		}
		for _, current := range rid {
			if (current < 'a' || current > 'z') && (current < 'A' || current > 'Z') && (current < '0' || current > '9') {
				return nil, fmt.Errorf("invalid simulcast layer id %q", rid)
			}
		}
		if _, duplicate := seen[rid]; duplicate {
			return nil, fmt.Errorf("duplicate simulcast layer id %q", rid)
		}
		parsed, err := strconv.ParseUint(rawBitrate, 10, 32)
		if err != nil || parsed == 0 || parsed > 100_000_000 {
			return nil, fmt.Errorf("invalid bitrate for simulcast layer %q", rid)
		}
		seen[rid] = struct{}{}
		layers = append(layers, SimulcastLayer{RID: rid, Bitrate: uint32(parsed)})
	}
	sort.Slice(layers, func(i, j int) bool { return layers[i].Bitrate < layers[j].Bitrate })
	return layers, nil
}
