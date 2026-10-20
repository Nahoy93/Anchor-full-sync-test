package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log"
	"net"
	"sync"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// UDP channel for realtime packets
//
// Positions, enemy snapshots, projectiles and sounds are sent many times a second and only the newest one matters.
// Over TCP a single lost segment holds up everything behind it until it is sent again; over UDP a lost packet is
// simply replaced by the next one. Everything else (items, flags, saves, chat...) always stays on TCP.
//
// The UDP socket listens on the same port number as TCP. After the handshake the server tells the client, in
// SERVER_INFO, a token that identifies it and (only over TLS) a key for AES-128-GCM. The client then sends
// UDP_HELLO datagrams until the server answers with UDP_ACK; a hello with "ready":true means the client receives
// our datagrams too, and from then on realtime packets for that client go over UDP. A client that stays silent for
// 15 seconds (it says hello every 5 seconds) gets everything over TCP again.
//
// Datagram: "AU", version 1, flags (bit 0 = encrypted), token (uint32 LE), sequence number (uint64 LE), payload.
// Encrypted payloads are AES-128-GCM with the 16 header bytes as additional data and the nonce
// direction (1 = client to server, 2 = server to client), 0, 0, 0, sequence number (uint64 LE).
// The same format is used by the game, see soh/Network/Anchor/UdpChannel.h.

const (
	udpHeaderSize     = 16
	udpTagSize        = 16
	udpKeySize        = 16
	udpMaxPayload     = 1400
	udpVersion        = 1
	udpFlagEncrypted  = 1
	dirClientToServer = 1
	dirServerToClient = 2
	udpSilenceTimeout = 15 * time.Second
)

// Packets where only the newest one matters, the same list as Anchor_IsRealtimePacket in the game
var realtimePacketTypes = map[string]bool{
	"PLAYER_UPDATE": true,
	"PLAYER_SFX":    true,
	"OCARINA_SFX":   true,
	"ENEMY_SYNC":    true,
	"PROJECTILES":   true,
	"DROP_MOVE":     true,
}

func isRealtimePacket(packetType string) bool {
	return realtimePacketTypes[packetType]
}

// udpEndpoint is one connection's UDP state. A reconnect gets a new token and key.
type udpEndpoint struct {
	token uint32
	key   []byte      // nil without TLS
	aead  cipher.AEAD // nil without TLS

	mu       sync.Mutex
	addr     *net.UDPAddr // where the client's datagrams come from, nil until the first hello
	recvSeq  uint64       // highest sequence number received from the client
	sendSeq  uint64       // last sequence number sent to the client
	ready    bool         // the client receives our datagrams
	lastSeen time.Time
}

// newUDPEndpoint makes the UDP state for a new connection, with a new random key when it's encrypted
func newUDPEndpoint(token uint32, encrypted bool) (*udpEndpoint, error) {
	if !encrypted {
		return newUDPEndpointWithKey(token, nil)
	}
	key := make([]byte, udpKeySize)
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return newUDPEndpointWithKey(token, key)
}

// newUDPEndpointWithKey uses the given key (nil = plain datagrams)
func newUDPEndpointWithKey(token uint32, key []byte) (*udpEndpoint, error) {
	endpoint := &udpEndpoint{token: token}
	if key == nil {
		return endpoint, nil
	}
	if len(key) != udpKeySize {
		return nil, errors.New("the UDP key must be 16 bytes")
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	endpoint.key = key
	endpoint.aead = aead
	return endpoint, nil
}

func udpNonce(direction byte, sequence uint64) []byte {
	nonce := make([]byte, 12)
	nonce[0] = direction
	binary.LittleEndian.PutUint64(nonce[4:], sequence)
	return nonce
}

func udpHeader(encrypted bool, token uint32, sequence uint64) []byte {
	header := make([]byte, udpHeaderSize)
	header[0] = 'A'
	header[1] = 'U'
	header[2] = udpVersion
	if encrypted {
		header[3] = udpFlagEncrypted
	}
	binary.LittleEndian.PutUint32(header[4:8], token)
	binary.LittleEndian.PutUint64(header[8:16], sequence)
	return header
}

// seal builds a datagram for the given direction
func (e *udpEndpoint) seal(direction byte, sequence uint64, payload []byte) []byte {
	header := udpHeader(e.aead != nil, e.token, sequence)
	if e.aead == nil {
		return append(header, payload...)
	}
	datagram := make([]byte, udpHeaderSize, udpHeaderSize+len(payload)+udpTagSize)
	copy(datagram, header)
	return e.aead.Seal(datagram, udpNonce(direction, sequence), payload, header)
}

var errBadDatagram = errors.New("not a valid datagram")

func parseUDPHeader(data []byte) (encrypted bool, token uint32, sequence uint64, err error) {
	if len(data) < udpHeaderSize || data[0] != 'A' || data[1] != 'U' || data[2] != udpVersion {
		return false, 0, 0, errBadDatagram
	}
	return data[3]&udpFlagEncrypted != 0, binary.LittleEndian.Uint32(data[4:8]), binary.LittleEndian.Uint64(data[8:16]), nil
}

// open checks and decrypts a datagram from the given direction
func (e *udpEndpoint) open(direction byte, data []byte) (sequence uint64, payload []byte, err error) {
	encrypted, token, sequence, err := parseUDPHeader(data)
	if err != nil {
		return 0, nil, err
	}
	// Never accept plain datagrams for an encrypted channel (or the other way around)
	if token != e.token || encrypted != (e.aead != nil) {
		return 0, nil, errBadDatagram
	}
	if !encrypted {
		return sequence, data[udpHeaderSize:], nil
	}
	if len(data) < udpHeaderSize+udpTagSize {
		return 0, nil, errBadDatagram
	}
	payload, err = e.aead.Open(nil, udpNonce(direction, sequence), data[udpHeaderSize:], data[:udpHeaderSize])
	return sequence, payload, err
}

func (e *udpEndpoint) keyHex() string {
	if e.key == nil {
		return ""
	}
	return hex.EncodeToString(e.key)
}

// MARK: - Server side

func (s *Server) startUDP() {
	addr := &net.UDPAddr{Port: s.config.Port}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		log.Printf("UDP: could not listen on port %d (%v), realtime packets stay on TCP", s.config.Port, err)
		return
	}
	s.udpConn = conn
	log.Printf("UDP: listening on port %d for realtime packets", s.config.Port)

	go s.udpLoop(conn)
}

func (s *Server) udpLoop(conn *net.UDPConn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in udpLoop: %v", r)
		}
	}()

	buf := make([]byte, 2048)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		data := make([]byte, n)
		copy(data, buf[:n])
		s.handleDatagram(data, addr)
	}
}

// newUDPToken returns a random token no other connection uses
func (s *Server) newUDPToken() uint32 {
	for {
		var b [4]byte
		if _, err := rand.Read(b[:]); err != nil {
			continue
		}
		token := binary.LittleEndian.Uint32(b[:])
		if token == 0 {
			continue
		}
		if _, taken := s.udpTokens.Load(token); !taken {
			return token
		}
	}
}

func (s *Server) handleDatagram(data []byte, addr *net.UDPAddr) {
	_, token, _, err := parseUDPHeader(data)
	if err != nil {
		return
	}
	value, ok := s.udpTokens.Load(token)
	if !ok {
		return
	}
	client := value.(*Client)

	client.mu.Lock()
	endpoint := client.udp
	client.mu.Unlock()
	if endpoint == nil || endpoint.token != token {
		return
	}

	sequence, payload, err := endpoint.open(dirClientToServer, data)
	if err != nil {
		return
	}

	endpoint.mu.Lock()
	// Late or repeated datagrams: a newer one already arrived
	if sequence <= endpoint.recvSeq {
		endpoint.mu.Unlock()
		return
	}
	endpoint.recvSeq = sequence
	endpoint.addr = addr
	endpoint.lastSeen = time.Now()
	endpoint.mu.Unlock()

	client.bytesIn.Add(uint64(len(data)))

	packet := string(payload)
	if !gjson.Valid(packet) {
		return
	}
	packetType := gjson.Get(packet, "type").String()

	if packetType == "UDP_HELLO" {
		endpoint.mu.Lock()
		endpoint.ready = gjson.Get(packet, "ready").Bool()
		endpoint.mu.Unlock()
		s.sendDatagram(client, endpoint, `{"type":"UDP_ACK","quiet":true}`, true)
		return
	}

	// Anything that isn't allowed to be lost has to come over TCP
	if !isRealtimePacket(packetType) {
		return
	}

	// The sender is whoever owns the token, whatever the packet says
	packet, _ = sjson.Set(packet, "clientId", client.id)
	client.handlePacket(packet)
}

// sendDatagram sends a packet to the client over UDP. Unless force is set, only when the client said it receives
// our datagrams and was heard from recently. Returns false when the packet has to go over TCP instead.
func (s *Server) sendDatagram(client *Client, endpoint *udpEndpoint, packet string, force bool) bool {
	conn := s.udpConn
	if conn == nil || len(packet) > udpMaxPayload {
		return false
	}

	endpoint.mu.Lock()
	if endpoint.addr == nil || (!force && (!endpoint.ready || time.Since(endpoint.lastSeen) > udpSilenceTimeout)) {
		endpoint.mu.Unlock()
		return false
	}
	endpoint.sendSeq++
	sequence := endpoint.sendSeq
	addr := endpoint.addr
	endpoint.mu.Unlock()

	datagram := endpoint.seal(dirServerToClient, sequence, []byte(packet))
	if _, err := conn.WriteToUDP(datagram, addr); err != nil {
		return false
	}
	client.bytesOut.Add(uint64(len(datagram)))
	return true
}

// udpStatus is a short description for the console
func (c *Client) udpStatus() string {
	c.mu.Lock()
	endpoint := c.udp
	c.mu.Unlock()
	if endpoint == nil {
		return "TCP only"
	}
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.ready && time.Since(endpoint.lastSeen) <= udpSilenceTimeout {
		return "UDP"
	}
	if endpoint.addr != nil {
		return "TCP (UDP one way only)"
	}
	return "TCP (no UDP yet)"
}
