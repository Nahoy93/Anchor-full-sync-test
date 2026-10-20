package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const sendQueueSize = 256

// Packets waiting in the queue are written together, up to this many bytes per write
const writeBatchSize = 64 * 1024

type Client struct {
	id           uint64
	conn         net.Conn
	sendCh       chan string // Outgoing packet queue, drained by the connection's writeLoop
	server       *Server
	room         *Room
	team         *Team
	state        string     // Client state, current scene, etc.
	mu           sync.Mutex // Mutex for safely updating state
	lastActivity time.Time

	tls bool         // the current connection is encrypted
	udp *udpEndpoint // the current connection's UDP channel, nil without UDP

	// From the client's pings, -1 = unknown
	rtt   int64
	hp    int64
	maxHp int64
	down  bool

	// Traffic, TCP and UDP together. The rates are updated every couple of seconds (trafficLoop).
	bytesIn      atomic.Uint64
	bytesOut     atomic.Uint64
	lastBytesIn  uint64
	lastBytesOut uint64
	rateIn       float64 // bytes per second
	rateOut      float64
}

func (c *Client) attachConnLocked(conn net.Conn, isTLS bool) {
	c.conn = conn
	c.sendCh = make(chan string, sendQueueSize)
	c.tls = isTLS

	// A new connection gets a new UDP token (and key)
	if c.udp != nil {
		c.server.udpTokens.Delete(c.udp.token)
		c.udp = nil
	}
	if c.server.udpConn != nil {
		endpoint, err := newUDPEndpoint(c.server.newUDPToken(), isTLS)
		if err != nil {
			log.Printf("Client %d: no UDP channel: %v", c.id, err)
		} else {
			c.udp = endpoint
			c.server.udpTokens.Store(endpoint.token, c)
		}
	}

	go c.writeLoop(conn, c.sendCh)
}

func (c *Client) writeLoop(conn net.Conn, ch chan string) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("Panic in writeLoop for client %d: %v", c.id, r)
		}
	}()

	buf := make([]byte, 0, writeBatchSize)
	for packet := range ch {
		// Everything that is already waiting goes out in one write (one TLS record, one syscall)
		buf = append(buf[:0], packet...)
		buf = append(buf, 0)
	batch:
		for len(buf) < writeBatchSize {
			select {
			case next, ok := <-ch:
				if !ok {
					break batch
				}
				buf = append(buf, next...)
				buf = append(buf, 0)
			default:
				break batch
			}
		}

		// Set write deadline to prevent blocking on dead connections
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err := conn.Write(buf)
		conn.SetWriteDeadline(time.Time{}) // Clear deadline

		if err != nil {
			c.disconnectConn(conn)
			return
		}
		c.bytesOut.Add(uint64(len(buf)))

		c.mu.Lock()
		c.lastActivity = time.Now()
		c.mu.Unlock()

		// Don't keep a huge buffer around after a big packet (a save state)
		if cap(buf) > 4*writeBatchSize {
			buf = make([]byte, 0, writeBatchSize)
		}
	}
}

func (c *Client) handlePacket(packet string) {
	c.mu.Lock()
	c.lastActivity = time.Now()
	c.mu.Unlock()

	packetType := gjson.Get(packet, "type").String()

	if !c.server.quietMode.Load() && !gjson.Get(packet, "quiet").Exists() {
		log.Printf("Client %d -> Server: %s\n", c.id, packetType)
	}

	if packetType == "UPDATE_CLIENT_STATE" {
		team := c.room.findOrCreateTeam(gjson.Get(packet, "state.teamId").String())

		c.mu.Lock()
		c.state = gjson.Get(packet, "state").Raw
		c.state, _ = sjson.Set(c.state, "clientId", c.id)
		c.team = team
		c.mu.Unlock()
	}

	if packetType == "GAME_COMPLETE" {
		c.server.gameCompleteCount.Add(1)
	}

	// Ping: remember the client's ping and hearts, answer with everyone's
	if packetType == "PING" {
		c.mu.Lock()
		c.rtt = gjson.Get(packet, "rtt").Int()
		c.hp = gjson.Get(packet, "hp").Int()
		c.maxHp = gjson.Get(packet, "maxHp").Int()
		c.down = gjson.Get(packet, "down").Bool()
		c.mu.Unlock()
		c.sendPacket(c.room.pongPacket(gjson.Get(packet, "t").Raw))
		return
	}

	// One packet for several clients: the copies are made here instead of by the sender
	targetClientIds := gjson.Get(packet, "targetClientIds")
	if targetClientIds.IsArray() {
		for _, id := range targetClientIds.Array() {
			if value, ok := c.room.clients.Load(id.Uint()); ok {
				if target := value.(*Client); target.id != c.id {
					target.sendPacket(packet)
				}
			}
		}
		return
	}

	targetClientId := gjson.Get(packet, "targetClientId")

	if targetClientId.Exists() {
		value, ok := c.room.clients.Load(targetClientId.Uint())
		if ok {
			targetClient := value.(*Client)
			targetClient.sendPacket(packet)
		}
		return
	}

	targetTeamId := gjson.Get(packet, "targetTeamId")

	if packetType == "REQUEST_TEAM_STATE" {
		if !targetTeamId.Exists() {
			return
		}

		team := c.room.findOrCreateTeam(targetTeamId.String())
		teamMemberOnline := false
		c.room.clients.Range(func(_, value interface{}) bool {
			client := value.(*Client)
			client.mu.Lock()
			if client.id != c.id && client.conn != nil && client.team == team && gjson.Get(client.state, "isSaveLoaded").Bool() {
				teamMemberOnline = true
			}
			client.mu.Unlock()
			return true
		})

		if teamMemberOnline {
			team.mu.Lock()
			team.clientIdsRequestingState = append(team.clientIdsRequestingState, c.id)
			team.mu.Unlock()
			team.broadcastPacket(packet)
			return
		}

		// Teammate is offline, see if we have a saved state for the team
		outgoingPacket := `{"type": "UPDATE_TEAM_STATE"}`
		team.mu.Lock()
		if team.state != "{}" {
			outgoingPacket, _ = sjson.SetRaw(outgoingPacket, "state", team.state)
		}
		withQueue, _ := sjson.Set(outgoingPacket, "queue", team.queue)
		queued := len(team.queue)
		team.mu.Unlock()

		if len(withQueue) <= MAX_PACKET_SIZE {
			outgoingPacket = withQueue
		} else {
			log.Printf("Team %s state plus %d queued packets is %d bytes, over the %d byte limit; sending state only",
				team.id, queued, len(withQueue), MAX_PACKET_SIZE)
			outgoingPacket, _ = sjson.Set(outgoingPacket, "queue", []string{})
		}

		c.sendPacket(outgoingPacket)
	} else if packetType == "UPDATE_TEAM_STATE" {
		if !targetTeamId.Exists() {
			return
		}

		team := c.room.findOrCreateTeam(targetTeamId.String())

		team.mu.Lock()
		clientIdsRequestingState := team.clientIdsRequestingState
		team.state = gjson.Get(packet, "state").Raw
		team.queue = []string{}
		team.droppedFromQueue = 0
		team.clientIdsRequestingState = []uint64{}
		team.mu.Unlock()
		c.room.markDirty()

		for _, clientId := range clientIdsRequestingState {
			if value, ok := c.room.clients.Load(clientId); ok {
				client := value.(*Client)
				client.sendPacket(packet)
			}
		}

	} else if packetType == "UPDATE_ROOM_STATE" {
		c.room.mu.Lock()
		c.room.state = gjson.Get(packet, "state").Raw
		c.room.mu.Unlock()
		c.room.markDirty()
		c.room.broadcastPacket(packet)
	} else if targetTeamId.Exists() {
		team := c.room.findOrCreateTeam(targetTeamId.String())
		addToQueue := gjson.Get(packet, "addToQueue")

		if addToQueue.Exists() && addToQueue.Bool() {
			team.enqueue(packet)
		}

		team.broadcastPacket(packet)
	} else {
		c.room.broadcastPacket(packet)
	}
}

func (c *Client) sendPacket(packet string) {
	packetType := gjson.Get(packet, "type").String()
	if !c.server.quietMode.Load() && !gjson.Get(packet, "quiet").Exists() {
		log.Printf("Client %d <- Server: %s\n", c.id, packetType)
	}

	// Realtime packets go over UDP once the client receives it
	if isRealtimePacket(packetType) {
		c.mu.Lock()
		endpoint := c.udp
		c.mu.Unlock()
		if endpoint != nil && c.server.sendDatagram(c, endpoint, packet, false) {
			return
		}
	}

	// Lock to prevent race condition with disconnect
	c.mu.Lock()
	conn := c.conn
	ch := c.sendCh
	full := false
	if conn != nil && ch != nil {
		select {
		case ch <- packet:
		default:
			full = true
		}
	}
	c.mu.Unlock()

	if full {
		// Queue full, the client isn't draining its socket, consider the session dead
		log.Printf("Client %d send queue full, disconnecting\n", c.id)
		c.disconnectConn(conn)
	}
}

func (c *Client) disconnect() {
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()

	c.disconnectConn(conn)
}

func (c *Client) disconnectConn(conn net.Conn) {
	if conn == nil {
		return
	}

	c.mu.Lock()
	if c.conn != conn {
		c.mu.Unlock()
		return
	}
	c.state, _ = sjson.Set(c.state, "online", false)
	c.state, _ = sjson.Set(c.state, "isSaveLoaded", false)
	c.conn = nil
	if c.udp != nil {
		c.server.udpTokens.Delete(c.udp.token)
		c.udp = nil
	}
	if c.sendCh != nil {
		close(c.sendCh)
		c.sendCh = nil
	}
	c.mu.Unlock()

	c.server.onlineClients.Delete(c.id)
}

// sendServerInfo tells a client what this server can do, right after its handshake. Clients that don't know the
// packet ignore it; clients that do use it to turn on fan-out (targetClientIds), pings and the UDP channel.
func (c *Client) sendServerInfo() {
	packet := `{"type":"SERVER_INFO","features":["targets","ping"]}`

	c.mu.Lock()
	endpoint := c.udp
	c.mu.Unlock()
	if endpoint != nil {
		packet, _ = sjson.Set(packet, "features.-1", "udp")
		// 0 = the same port number the client connected to over TCP
		packet, _ = sjson.Set(packet, "udp.port", 0)
		packet, _ = sjson.Set(packet, "udp.token", endpoint.token)
		// The key only ever goes over TLS
		if key := endpoint.keyHex(); key != "" {
			packet, _ = sjson.Set(packet, "udp.key", key)
		}
	}

	c.sendPacket(packet)
}

// networkSummary is one line about the client's connection for the console
func (c *Client) networkSummary() string {
	c.mu.Lock()
	online := c.conn != nil
	rtt := c.rtt
	isTLS := c.tls
	queue := 0
	if c.sendCh != nil {
		queue = len(c.sendCh)
	}
	rateIn, rateOut := c.rateIn, c.rateOut
	c.mu.Unlock()

	if !online {
		return "offline"
	}
	ping := "ping ?"
	if rtt >= 0 {
		ping = fmt.Sprintf("ping %d ms", rtt)
	}
	encryption := "no TLS"
	if isTLS {
		encryption = "TLS"
	}
	return fmt.Sprintf("%s, in %.1f KB/s, out %.1f KB/s, queue %d/%d, %s, %s", ping, rateIn/1024, rateOut/1024,
		queue, sendQueueSize, encryption, c.udpStatus())
}

func (c *Client) sendRoomState() {
	c.room.mu.Lock()
	packet, _ := sjson.SetRaw(`{"type":"UPDATE_ROOM_STATE"}`, "state", c.room.state)
	c.room.mu.Unlock()

	c.sendPacket(packet)
}
