package main

import (
	"crypto/subtle"
	"flag"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"
)

// Config holds the self-hosting options. Every option can be set either with a command line flag
// or with an environment variable (the flag wins if both are given).
//
//	-port      / PORT             TCP port to listen on (default 43383)
//	-password  / ANCHOR_PASSWORD  Password required to join (empty = open server)
type Config struct {
	Port     int
	Password string
}

func LoadConfig() Config {
	cfg := Config{Port: 43383}

	if env := os.Getenv("PORT"); env != "" {
		if p, err := strconv.Atoi(env); err == nil {
			cfg.Port = p
		}
	}
	cfg.Password = os.Getenv("ANCHOR_PASSWORD")

	flag.IntVar(&cfg.Port, "port", cfg.Port, "TCP port to listen on")
	flag.StringVar(&cfg.Password, "password", cfg.Password, "password clients must send to join (empty = no password)")
	flag.Parse()

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
