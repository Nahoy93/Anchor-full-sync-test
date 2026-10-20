package main

import (
	"crypto/subtle"
	"flag"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Config holds the self-hosting options. Every option can be set either with a command line flag
// or with an environment variable (the flag wins if both are given).
//
//	-port         / PORT                TCP and UDP port to listen on (default 43383)
//	-password     / ANCHOR_PASSWORD     Password required to join (empty = open server)
//	-tls          / ANCHOR_TLS          Accept encrypted (TLS) connections next to plain ones (default on, 0 = off)
//	-require-tls  / ANCHOR_REQUIRE_TLS  Only accept encrypted connections (default off)
//	-tls-cert     / ANCHOR_TLS_CERT     Certificate file, created on the first start (default tls/cert.pem)
//	-tls-key      / ANCHOR_TLS_KEY      Private key file, created on the first start (default tls/key.pem)
//	-udp          / ANCHOR_UDP          Send realtime packets (positions, enemies) over UDP when it works (default on)
type Config struct {
	Port       int
	Password   string
	TLS        bool
	RequireTLS bool
	TLSCert    string
	TLSKey     string
	UDP        bool
}

// envBool reads an on/off environment variable: 1/true/on/yes or 0/false/off/no
func envBool(name string, def bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "on", "yes":
		return true
	case "0", "false", "off", "no":
		return false
	}
	return def
}

func envString(name, def string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return def
}

func LoadConfig() Config {
	cfg := Config{Port: 43383}

	if env := os.Getenv("PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil {
			cfg.Port = p
		}
	}
	cfg.Password = os.Getenv("ANCHOR_PASSWORD")
	cfg.TLS = envBool("ANCHOR_TLS", true)
	cfg.RequireTLS = envBool("ANCHOR_REQUIRE_TLS", false)
	cfg.TLSCert = envString("ANCHOR_TLS_CERT", "tls/cert.pem")
	cfg.TLSKey = envString("ANCHOR_TLS_KEY", "tls/key.pem")
	cfg.UDP = envBool("ANCHOR_UDP", true)

	flag.IntVar(&cfg.Port, "port", cfg.Port, "TCP and UDP port to listen on")
	flag.StringVar(&cfg.Password, "password", cfg.Password, "password clients must send to join (empty = no password)")
	flag.BoolVar(&cfg.TLS, "tls", cfg.TLS, "accept encrypted (TLS) connections next to plain ones")
	flag.BoolVar(&cfg.RequireTLS, "require-tls", cfg.RequireTLS, "only accept encrypted (TLS) connections")
	flag.StringVar(&cfg.TLSCert, "tls-cert", cfg.TLSCert, "TLS certificate file (created on the first start)")
	flag.StringVar(&cfg.TLSKey, "tls-key", cfg.TLSKey, "TLS private key file (created on the first start)")
	flag.BoolVar(&cfg.UDP, "udp", cfg.UDP, "send realtime packets (positions, enemies) over UDP when it works")
	flag.Parse()

	if cfg.RequireTLS && !cfg.TLS {
		log.Fatal("-require-tls needs TLS turned on (-tls / ANCHOR_TLS)")
	}

	if cfg.Port <= 0 || cfg.Port > 65535 {
		log.Fatalf("Invalid port %d", cfg.Port)
	}

	return cfg
}

func (s *Server) passwordMatches(given string) bool {
	if s.config.Password == "" {
		return true
	}
	return subtle.ConstantTimeCompare([]byte(given), []byte(s.config.Password)) == 1
}

// Simple brute-force protection: after MAX_FAILED_LOGINS wrong passwords from one IP within
// FAILED_LOGIN_WINDOW, further handshakes from that IP are refused until the window has passed.
const MAX_FAILED_LOGINS = 10
const FAILED_LOGIN_WINDOW = 5 * time.Minute

type loginLimiter struct {
	mu       sync.Mutex
	failures map[string][]time.Time
}

func newLoginLimiter() *loginLimiter {
	return &loginLimiter{failures: map[string][]time.Time{}}
}

func remoteIP(conn net.Conn) string {
	host, _, err := net.SplitHostPort(conn.RemoteAddr().String())
	if err != nil {
		return conn.RemoteAddr().String()
	}
	return host
}

func (l *loginLimiter) prune(ip string) []time.Time {
	recent := l.failures[ip][:0]
	for _, t := range l.failures[ip] {
		if time.Since(t) < FAILED_LOGIN_WINDOW {
			recent = append(recent, t)
		}
	}
	if len(recent) == 0 {
		delete(l.failures, ip)
	} else {
		l.failures[ip] = recent
	}
	return recent
}

func (l *loginLimiter) isBlocked(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.prune(ip)) >= MAX_FAILED_LOGINS
}

func (l *loginLimiter) recordFailure(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	recent := l.prune(ip)
	// The game reconnects automatically right after being rejected, don't count those retries
	if len(recent) > 0 && time.Since(recent[len(recent)-1]) < 3*time.Second {
		return
	}
	l.failures[ip] = append(recent, time.Now())
}

func (l *loginLimiter) reset(ip string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.failures, ip)
}
