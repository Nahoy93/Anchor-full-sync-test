package main

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TLS (encrypted connections)
//
// TLS and plain connections share the port: the first byte of a TLS connection is always 0x16 (a handshake record),
// while a plain connection starts with the JSON of its handshake. With ANCHOR_REQUIRE_TLS=1 plain connections are
// turned away with a message.
//
// A self-hosted server has no certificate from a certificate authority, so the first start creates a self-signed one
// in tls/ and prints its fingerprint. The game remembers that fingerprint the first time it connects (like SSH does),
// or players can paste it into the Anchor menu beforehand. Keep the tls/ folder: a new certificate means everyone has
// to press "Forget Saved Certificates" once.

const tlsPeekTimeout = 60 * time.Second
const tlsHandshakeTimeout = 15 * time.Second

// peekedConn is a connection whose first bytes were already read into a buffer
type peekedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c *peekedConn) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// loadOrCreateCertificate loads the server certificate, or creates a self-signed one the first time
func loadOrCreateCertificate(certPath, keyPath string) (tls.Certificate, bool, error) {
	certExists, keyExists := fileExists(certPath), fileExists(keyPath)
	if certExists && keyExists {
		cert, err := tls.LoadX509KeyPair(certPath, keyPath)
		return cert, false, err
	}
	if certExists != keyExists {
		return tls.Certificate{}, false, fmt.Errorf("only one of %s and %s exists, remove it to create a new certificate", certPath, keyPath)
	}

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, false, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 127))
	if err != nil {
		return tls.Certificate{}, false, err
	}
	template := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "Anchor co-op server"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().AddDate(30, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, false, err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return tls.Certificate{}, false, err
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	for _, dir := range []string{filepath.Dir(certPath), filepath.Dir(keyPath)} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return tls.Certificate{}, false, err
		}
	}
	if err := os.WriteFile(keyPath, keyPEM, 0600); err != nil {
		return tls.Certificate{}, false, err
	}
	if err := os.WriteFile(certPath, certPEM, 0644); err != nil {
		return tls.Certificate{}, false, err
	}

	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	return cert, true, err
}

// certificateFingerprint is the SHA-256 of the certificate, as the game shows and checks it (AB:CD:...)
func certificateFingerprint(cert tls.Certificate) string {
	if len(cert.Certificate) == 0 {
		return ""
	}
	sum := sha256.Sum256(cert.Certificate[0])
	parts := make([]string, len(sum))
	for i, b := range sum {
		parts[i] = fmt.Sprintf("%02X", b)
	}
	return strings.Join(parts, ":")
}

func (s *Server) setupTLS() error {
	cert, created, err := loadOrCreateCertificate(s.config.TLSCert, s.config.TLSKey)
	if err != nil {
		return err
	}
	s.tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	}
	s.tlsFingerprint = certificateFingerprint(cert)
	if created {
		fmt.Println("TLS: created a new certificate in", s.config.TLSCert)
	}
	return nil
}

var errNoFirstByte = errors.New("the client sent nothing")

// acceptConn looks at the first byte of a new connection and starts TLS when the client speaks it
func (s *Server) acceptConn(conn net.Conn) (net.Conn, bool, error) {
	reader := bufio.NewReaderSize(conn, 4096)
	conn.SetReadDeadline(time.Now().Add(tlsPeekTimeout))
	first, err := reader.Peek(1)
	conn.SetReadDeadline(time.Time{})
	if err != nil || len(first) == 0 {
		return nil, false, errNoFirstByte
	}

	wrapped := &peekedConn{Conn: conn, reader: reader}
	if first[0] != 0x16 {
		return wrapped, false, nil
	}
	if s.tlsConfig == nil {
		return nil, false, errors.New("the client wants TLS, but TLS is turned off on this server")
	}

	tlsConn := tls.Server(wrapped, s.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(tlsHandshakeTimeout))
	if err := tlsConn.Handshake(); err != nil {
		return nil, false, fmt.Errorf("TLS handshake failed: %w", err)
	}
	tlsConn.SetDeadline(time.Time{})
	return tlsConn, true, nil
}
