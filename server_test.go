package main

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/tidwall/gjson"
)

// The server writes stats.json and sessions/ into the working directory: run the tests in a temporary one
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "anchor-test")
	if err != nil {
		panic(err)
	}
	if err := os.Chdir(dir); err != nil {
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// freePort finds a port that is free for both TCP and UDP
func freePort(t *testing.T) int {
	for i := 0; i < 20; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		l.Close()
		u, err := net.ListenUDP("udp", &net.UDPAddr{Port: port})
		if err != nil {
			continue
		}
		u.Close()
		return port
	}
	t.Fatal("no free port")
	return 0
}

func startTestServer(t *testing.T, config Config) *Server {
	t.Helper()
	config.Port = freePort(t)
	dir := t.TempDir()
	config.TLSCert = filepath.Join(dir, "tls", "cert.pem")
	config.TLSKey = filepath.Join(dir, "tls", "key.pem")

	s := NewServer(config)
	errChan := make(chan error, 10)
	go s.Start(errChan)

	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", config.Port))
		if err == nil {
			conn.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Start sets these up before it accepts connections
	t.Cleanup(func() {
		s.listener.Close()
		if s.udpConn != nil {
			s.udpConn.Close()
		}
	})
	return s
}

// testClient is a minimal game client: null terminated JSON over TCP (or TLS), plus the UDP channel
type testClient struct {
	t        *testing.T
	conn     net.Conn
	reader   *bufio.Reader
	id       uint64
	info     string // SERVER_INFO
	udp      *net.UDPConn
	endpoint *udpEndpoint
	sendSeq  uint64
	recvSeq  uint64
}

func dialTestClient(t *testing.T, s *Server, useTLS bool, name string) *testClient {
	t.Helper()
	addr := fmt.Sprintf("127.0.0.1:%d", s.config.Port)
	var conn net.Conn
	var err error
	if useTLS {
		conn, err = tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	} else {
		conn, err = net.Dial("tcp", addr)
	}
	if err != nil {
		t.Fatal(err)
	}
	c := &testClient{t: t, conn: conn, reader: bufio.NewReader(conn)}
	t.Cleanup(func() {
		conn.Close()
		if c.udp != nil {
			c.udp.Close()
		}
	})

	c.send(fmt.Sprintf(`{"type":"HANDSHAKE","clientId":0,"roomId":"test-room","password":"%s","clientState":{"name":"%s","teamId":"default","online":true}}`,
		s.config.Password, name))

	for {
		packet := c.read()
		switch gjson.Get(packet, "type").String() {
		case "ALL_CLIENT_STATE":
			for _, state := range gjson.Get(packet, "state").Array() {
				if state.Get("self").Bool() {
					c.id = state.Get("clientId").Uint()
				}
			}
		case "SERVER_INFO":
			c.info = packet
			return c
		case "SERVER_MESSAGE", "DISABLE_ANCHOR":
			c.info = packet
			return c
		}
	}
}

func (c *testClient) send(packet string) {
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.conn.Write(append([]byte(packet), 0)); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testClient) read() string {
	c.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	data, err := c.reader.ReadBytes(0)
	if err != nil {
		c.t.Fatalf("read: %v", err)
	}
	return string(bytes.TrimSuffix(data, []byte{0}))
}

// readType skips packets until one of the given type arrives
func (c *testClient) readType(packetType string) string {
	for {
		packet := c.read()
		if gjson.Get(packet, "type").String() == packetType {
			return packet
		}
	}
}

func (c *testClient) openUDP(s *Server) {
	info := gjson.Get(c.info, "udp")
	if !info.Exists() {
		c.t.Fatalf("no udp in SERVER_INFO: %s", c.info)
	}
	var key []byte
	if keyHex := info.Get("key").String(); keyHex != "" {
		key, _ = hex.DecodeString(keyHex)
	}
	endpoint, err := newUDPEndpointWithKey(uint32(info.Get("token").Uint()), key)
	if err != nil {
		c.t.Fatal(err)
	}
	c.endpoint = endpoint
	port := int(info.Get("port").Int())
	if port == 0 {
		port = s.config.Port
	}
	conn, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
	if err != nil {
		c.t.Fatal(err)
	}
	c.udp = conn
}

func (c *testClient) sendUDP(packet string) {
	c.sendSeq++
	datagram := c.endpoint.seal(dirClientToServer, c.sendSeq, []byte(packet))
	if _, err := c.udp.Write(datagram); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testClient) sendUDPWithSeq(seq uint64, packet string) {
	datagram := c.endpoint.seal(dirClientToServer, seq, []byte(packet))
	c.udp.Write(datagram)
}

func (c *testClient) readUDP() (string, error) {
	buf := make([]byte, 2048)
	c.udp.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := c.udp.Read(buf)
	if err != nil {
		return "", err
	}
	seq, payload, err := c.endpoint.open(dirServerToClient, buf[:n])
	if err != nil {
		return "", err
	}
	if seq <= c.recvSeq {
		return "", fmt.Errorf("old sequence %d", seq)
	}
	c.recvSeq = seq
	return string(payload), nil
}

func (c *testClient) udpHandshake() {
	c.sendUDP(`{"type":"UDP_HELLO","ready":false}`)
	ack, err := c.readUDP()
	if err != nil || gjson.Get(ack, "type").String() != "UDP_ACK" {
		c.t.Fatalf("no UDP_ACK: %q %v", ack, err)
	}
	c.sendUDP(`{"type":"UDP_HELLO","ready":true}`)
	if ack, err = c.readUDP(); err != nil || gjson.Get(ack, "type").String() != "UDP_ACK" {
		c.t.Fatalf("no second UDP_ACK: %q %v", ack, err)
	}
}

func TestUDPDatagramRoundTrip(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		endpoint, err := newUDPEndpoint(1234, encrypted)
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte(`{"type":"PLAYER_UPDATE"}`)
		datagram := endpoint.seal(dirClientToServer, 7, payload)
		seq, opened, err := endpoint.open(dirClientToServer, datagram)
		if err != nil || seq != 7 || string(opened) != string(payload) {
			t.Fatalf("encrypted=%v: %v %d %q", encrypted, err, seq, opened)
		}
		if binary.LittleEndian.Uint32(datagram[4:8]) != 1234 {
			t.Fatal("token not in header")
		}
		if encrypted {
			// The wrong direction (a reflected datagram) or a changed byte must not open
			if _, _, err := endpoint.open(dirServerToClient, datagram); err == nil {
				t.Fatal("opened with the wrong direction")
			}
			datagram[len(datagram)-1] ^= 1
			if _, _, err := endpoint.open(dirClientToServer, datagram); err == nil {
				t.Fatal("opened a changed datagram")
			}
		}
	}
}

func TestTLSUDPFanoutAndPing(t *testing.T) {
	s := startTestServer(t, Config{TLS: true, UDP: true, Password: "secret"})

	a := dialTestClient(t, s, true, "A")
	b := dialTestClient(t, s, false, "B")
	c := dialTestClient(t, s, false, "C")

	for _, client := range []*testClient{a, b, c} {
		features := gjson.Get(client.info, "features").String()
		if !strings.Contains(features, "targets") || !strings.Contains(features, "ping") || !strings.Contains(features, "udp") {
			t.Fatalf("features: %s", client.info)
		}
	}
	// The UDP key only goes over TLS
	if gjson.Get(a.info, "udp.key").String() == "" {
		t.Fatalf("no key over TLS: %s", a.info)
	}
	if gjson.Get(b.info, "udp.key").Exists() {
		t.Fatalf("key over plain TCP: %s", b.info)
	}

	a.openUDP(s)
	b.openUDP(s)
	a.udpHandshake()
	b.udpHandshake()

	// A realtime packet over UDP to B and C: B gets it over UDP, C (no UDP) over TCP
	a.sendUDP(fmt.Sprintf(`{"type":"PLAYER_UPDATE","clientId":999,"x":1,"targetClientIds":[%d,%d],"quiet":true}`, b.id, c.id))
	got, err := b.readUDP()
	if err != nil || gjson.Get(got, "x").Int() != 1 {
		t.Fatalf("B over UDP: %q %v", got, err)
	}
	// The server puts in the real sender
	if gjson.Get(got, "clientId").Uint() != a.id {
		t.Fatalf("sender not corrected: %s", got)
	}
	got = c.readType("PLAYER_UPDATE")
	if gjson.Get(got, "x").Int() != 1 {
		t.Fatalf("C over TCP: %s", got)
	}

	// A packet over TCP for A arrives encrypted over UDP
	b.send(fmt.Sprintf(`{"type":"PLAYER_UPDATE","clientId":%d,"x":2,"targetClientIds":[%d],"quiet":true}`, b.id, a.id))
	if got, err = a.readUDP(); err != nil || gjson.Get(got, "x").Int() != 2 {
		t.Fatalf("A over encrypted UDP: %q %v", got, err)
	}

	// Packets that may not be lost are refused over UDP
	a.sendUDP(fmt.Sprintf(`{"type":"GIVE_ITEM","clientId":%d,"targetClientIds":[%d],"quiet":true}`, a.id, b.id))
	// Replayed datagrams are dropped
	a.sendUDPWithSeq(1, fmt.Sprintf(`{"type":"PLAYER_UPDATE","x":3,"targetClientIds":[%d],"quiet":true}`, b.id))
	if got, err := b.readUDP(); err == nil {
		t.Fatalf("got a refused or replayed packet: %s", got)
	}

	// Non-realtime packets with targetClientIds over TCP
	b.send(fmt.Sprintf(`{"type":"GIVE_ITEM","clientId":%d,"y":5,"targetClientIds":[%d,%d]}`, b.id, a.id, c.id))
	if got := a.readType("GIVE_ITEM"); gjson.Get(got, "y").Int() != 5 {
		t.Fatalf("A: %s", got)
	}
	if got := c.readType("GIVE_ITEM"); gjson.Get(got, "y").Int() != 5 {
		t.Fatalf("C: %s", got)
	}

	// Ping: the answer has the time and everyone's ping and hearts
	b.send(`{"type":"PING","t":12345,"rtt":42,"hp":48,"maxHp":160,"down":false,"quiet":true}`)
	pong := b.readType("PONG")
	if gjson.Get(pong, "t").Int() != 12345 {
		t.Fatalf("pong time: %s", pong)
	}
	entry := gjson.Get(pong, fmt.Sprintf("clients.%d", b.id)).Array()
	if len(entry) != 3 || entry[0].Int() != 42 || entry[1].Int() != 48 || entry[2].Int() != 160 {
		t.Fatalf("pong clients: %s", pong)
	}
	if !gjson.Get(pong, fmt.Sprintf("clients.%d", a.id)).Exists() {
		t.Fatalf("pong misses A: %s", pong)
	}
}

func TestWrongUDPKeyIsIgnored(t *testing.T) {
	s := startTestServer(t, Config{TLS: true, UDP: true})
	a := dialTestClient(t, s, true, "A")
	a.openUDP(s)

	// Same token, other key
	forged, _ := newUDPEndpoint(a.endpoint.token, true)
	datagram := forged.seal(dirClientToServer, 1, []byte(`{"type":"UDP_HELLO","ready":true}`))
	a.udp.Write(datagram)
	// Plain datagram for an encrypted channel
	plain := &udpEndpoint{token: a.endpoint.token}
	a.udp.Write(plain.seal(dirClientToServer, 2, []byte(`{"type":"UDP_HELLO","ready":true}`)))

	if got, err := a.readUDP(); err == nil {
		t.Fatalf("forged hello was answered: %s", got)
	}
}

func TestRequireTLS(t *testing.T) {
	s := startTestServer(t, Config{TLS: true, RequireTLS: true, UDP: false})

	plain := dialTestClient(t, s, false, "plain")
	if gjson.Get(plain.info, "type").String() != "SERVER_MESSAGE" {
		t.Fatalf("plain connection not refused: %s", plain.info)
	}

	secure := dialTestClient(t, s, true, "secure")
	if gjson.Get(secure.info, "type").String() != "SERVER_INFO" {
		t.Fatalf("TLS connection refused: %s", secure.info)
	}
	if strings.Contains(gjson.Get(secure.info, "features").String(), "udp") {
		t.Fatalf("udp offered while off: %s", secure.info)
	}
}

func TestOldClientWithoutServerInfoSupport(t *testing.T) {
	// A client that never pings or uses UDP keeps working exactly like before: plain packets with targetClientId
	s := startTestServer(t, Config{TLS: false, UDP: true})
	a := dialTestClient(t, s, false, "A")
	b := dialTestClient(t, s, false, "B")
	a.send(fmt.Sprintf(`{"type":"PLAYER_UPDATE","clientId":%d,"x":7,"targetClientId":%d,"quiet":true}`, a.id, b.id))
	if got := b.readType("PLAYER_UPDATE"); gjson.Get(got, "x").Int() != 7 {
		t.Fatalf("B: %s", got)
	}
}

func TestCertificateIsKeptAndFingerprintMatches(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath := filepath.Join(dir, "tls", "cert.pem"), filepath.Join(dir, "tls", "key.pem")
	cert, created, err := loadOrCreateCertificate(certPath, keyPath)
	if err != nil || !created {
		t.Fatalf("create: %v %v", created, err)
	}
	again, created, err := loadOrCreateCertificate(certPath, keyPath)
	if err != nil || created {
		t.Fatalf("load: %v %v", created, err)
	}
	if certificateFingerprint(cert) != certificateFingerprint(again) {
		t.Fatal("certificate changed between starts")
	}
	if len(certificateFingerprint(cert)) != 32*3-1 {
		t.Fatalf("fingerprint format: %s", certificateFingerprint(cert))
	}
	info, _ := os.Stat(keyPath)
	if info.Mode().Perm()&0077 != 0 {
		t.Fatalf("key file readable by others: %v", info.Mode())
	}
}
