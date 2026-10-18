package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const JSON_TEMPLATE = `{"gameCompleteCount":0,"onlineCount":0,"lastStatsHeartbeat":"","uniqueCount":0,"pid":0}`
const INACTIVITY_TIMEOUT = 5 * time.Minute
const HEARTBEAT = 30 * time.Second
const MAX_PACKET_SIZE = 8 * 1024 * 1024
const INITIAL_SCAN_BUFFER = 64 * 1024

type Server struct {
	config            Config
	logins            *loginLimiter
	listener          net.Listener
	quietMode         atomic.Bool
	onlineClients     sync.Map
	rooms             sync.Map
	gameCompleteCount atomic.Uint64
	nextClientId      atomic.Uint64
}

func NewServer(config Config) *Server {
	s := &Server{
		config:            config,
		logins:            newLoginLimiter(),
		onlineClients:     sync.Map{},
		quietMode:         atomic.Bool{},
		rooms:             sync.Map{},
		gameCompleteCount: atomic.Uint64{},
		nextClientId:      atomic.Uint64{},
	}

	s.quietMode.Store(true)

	return s
}

func (s *Server) Start(errChan chan error) {
	addr := fmt.Sprintf(":%d", s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	s.listener = listener

	go s.cleanupInactiveRooms(errChan)
	go s.heartbeat(errChan)
	go s.parseStats(errChan)
	go s.statsHeartbeat(errChan)
	go s.saveDirtySessions(errChan)

	log.Println("Server running on", addr)
	if s.config.Password != "" {
		log.Println("Password protection: ON")
	} else {
		log.Println("Password protection: OFF (anyone can join, set -password or ANCHOR_PASSWORD)")
	}
	log.Println("Quiet mode:", s.quietMode.Load())

	for {
		conn, err := listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				log.Println("Error with listener:", err)
				break
			}
			log.Println("Error accepting connection:", err)
			conn.Close()
			continue
		}

		go s.handleConnection(conn, errChan)
	}
}

func (s *Server) parseStats(errChan chan error) {
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in parseStats: %v", r)
		}
	}()

	value, err := os.ReadFile("stats.json")
	if err != nil {
		log.Println("Error reading stats.json file:", err)
	}

	//input values into their repective fields of the server
	s.gameCompleteCount.Store(gjson.Get(string(value), "gameCompleteCount").Uint())
	s.nextClientId.Store(gjson.Get(string(value), "uniqueCount").Uint())

	// Save stats immediately to update lastStatsHeartbeat
	s.saveStats()
}

func (s *Server) onlineCount() int {
	var count int
	s.onlineClients.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	return count
}

func (s *Server) saveStats() {
	value, _ := sjson.Set(JSON_TEMPLATE, "gameCompleteCount", s.gameCompleteCount.Load())
	value, _ = sjson.Set(value, "uniqueCount", s.nextClientId.Load())
	value, _ = sjson.Set(value, "onlineCount", s.onlineCount())
	value, _ = sjson.Set(value, "lastStatsHeartbeat", time.Now().UnixMilli())
	value, _ = sjson.Set(value, "pid", os.Getpid())

	err := os.WriteFile("./stats.json", []byte(value), 0644)

	if err != nil {
		log.Println("Error writing json to file: ", err)
	}
}

func (s *Server) cleanupInactiveRooms(errChan chan error) {
	ticker := time.NewTicker(HEARTBEAT)
	defer ticker.Stop()
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in cleanupInactiveRooms: %v", r)
		}
	}()

	for range ticker.C {
		s.rooms.Range(func(id, value interface{}) bool {
			room := value.(*Room)
			lastActivity := room.GetLastActivity()
			if time.Since(lastActivity) > INACTIVITY_TIMEOUT {
				log.Println("Room", id, "has been inactive for too long, unloading it (the session stays saved)")
				if err := room.save(); err != nil {
					log.Printf("Could not save session %s: %v", room.id, err)
				}
				s.rooms.Delete(id)
			}
			return true
		})
	}
}

func (s *Server) statsHeartbeat(errChan chan error) {
	ticker := time.NewTicker(HEARTBEAT)
	defer ticker.Stop()
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in statsHeartbeat: %v", r)
		}
	}()

	for range ticker.C {
		s.saveStats()
	}
}

func (s *Server) heartbeat(errChan chan error) {
	ticker := time.NewTicker(HEARTBEAT)
	defer ticker.Stop()
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in heartbeat: %v", r)
		}
	}()

	for range ticker.C {
		log.Println("Clients Online & Threads Running", s.onlineCount(), runtime.NumGoroutine())

		s.onlineClients.Range(func(_, value interface{}) bool {
			client := value.(*Client)
			client.mu.Lock()
			idle := time.Since(client.lastActivity) > HEARTBEAT
			client.mu.Unlock()
			if idle {
				client.sendPacket(`{"type":"HEARTBEAT","quiet":true}`)
			}
			return true
		})
	}
}

func (s *Server) handleConnection(conn net.Conn, errChan chan error) {
	defer conn.Close()
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in handleConnection: %v", r)
		}
	}()

	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, INITIAL_SCAN_BUFFER), MAX_PACKET_SIZE)
	scanner.Split(splitNullByte)

	var client *Client

	for scanner.Scan() {
		packet := scanner.Text()

		if !gjson.Valid(packet) {
			log.Printf("Invalid JSON packet: %s\n", packet)
			continue
		}

		packetTypeWrapped := gjson.Get(packet, "type")
		if !packetTypeWrapped.Exists() {
			log.Println("Packet missing type")
			continue
		}

		packetType := packetTypeWrapped.String()

		// Health check
		if packetType == "STATS" {
			outgoingPacket, _ := sjson.Set(`{"type":"STATS"}`, "uniqueCount", s.nextClientId.Load())
			outgoingPacket, _ = sjson.Set(outgoingPacket, "gameCompleteCount", s.gameCompleteCount.Load())
			outgoingPacket, _ = sjson.Set(outgoingPacket, "onlineCount", s.onlineCount())
			conn.Write(append([]byte(outgoingPacket), 0))
			continue
		}

		if client == nil {
			if packetType != "HANDSHAKE" {
				log.Println("Client must handshake first")
				continue
			}

			ip := remoteIP(conn)
			if s.logins.isBlocked(ip) {
				log.Printf("Refusing handshake from %s: too many wrong passwords\n", ip)
				rejectConnection(conn, "Too many wrong passwords. Try again in a few minutes.")
				return
			}
			if !s.passwordMatches(gjson.Get(packet, "password").String()) {
				s.logins.recordFailure(ip)
				log.Printf("Wrong password from %s\n", ip)
				rejectConnection(conn, "Wrong server password. Check the Password field in the Anchor menu.")
				return
			}
			s.logins.reset(ip)

			client = s.findOrCreateClient(packet, conn)
			log.Printf("Client %v Connected\n", client.id)
			client.room.broadcastAllClientState()
			client.sendRoomState()
		} else {
			client.handlePacket(packet)
		}
	}

	if client != nil {
		client.disconnectConn(conn)
		client.room.broadcastAllClientState()

		if err := scanner.Err(); err != nil {
			if errors.Is(err, bufio.ErrTooLong) {
				log.Printf("Client %v sent a packet over the %d byte limit, disconnecting", client.id, MAX_PACKET_SIZE)
			} else {
				log.Printf("Client %v disconnected with error: %v", client.id, err)
			}
		} else {
			log.Printf("Client %v disconnected\n", client.id)
		}
	} else {
		log.Println("Unknown client disconnected.")
	}

}

func (s *Server) findOrCreateClient(packet string, conn net.Conn) *Client {
	clientId := gjson.Get(packet, "clientId").Uint()
	roomId := gjson.Get(packet, "roomId").String()

	takeover := false
	if clientId != 0 {
		if value, ok := s.onlineClients.Load(clientId); ok {
			existing := value.(*Client)
			if existing.room.id == roomId {
				takeover = true
				log.Printf("Client %v reconnected, closing stale session\n", clientId)
				existing.disconnect()
			} else {
				clientId = 0
			}
		}
	}

	// Check if the client id is already in use or is 0 and look for a new one
	for !takeover {
		if _, ok := s.onlineClients.Load(clientId); !ok && clientId != 0 {
			break
		}
		clientId = s.nextClientId.Add(1)
	}

	room := s.findOrCreateRoom(packet, clientId)
	team := room.findOrCreateTeam(gjson.Get(packet, "clientState.teamId").String())

	var client *Client
	loadedClient, ok := room.clients.Load(clientId)
	clientState, _ := sjson.Set(gjson.Get(packet, "clientState").Raw, "clientId", clientId)
	if ok {
		client = loadedClient.(*Client)
		client.mu.Lock()
		client.attachConnLocked(conn)
		client.state = clientState
		client.team = team
		client.lastActivity = time.Now()
		client.mu.Unlock()
	} else {
		client = &Client{
			id:           clientId,
			server:       s,
			room:         room,
			team:         team,
			state:        clientState,
			lastActivity: time.Now(),
		}
		client.mu.Lock()
		client.attachConnLocked(conn)
		client.mu.Unlock()
		room.clients.Store(clientId, client)
	}

	s.onlineClients.Store(clientId, client)

	return client
}

func (s *Server) findOrCreateRoom(packet string, clientId uint64) *Room {
	roomId := gjson.Get(packet, "roomId").String()

	room, ok := s.rooms.Load(roomId)
	if !ok {
		// Continue a saved session if there is one, otherwise start a new one
		newRoom := loadSession(roomId)
		if newRoom == nil {
			newRoom = NewRoom(roomId, clientId, packet)
		}
		room, _ = s.rooms.LoadOrStore(roomId, newRoom)
	}

	return room.(*Room)
}

func splitNullByte(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if i := bytes.IndexByte(data, 0); i >= 0 {
		return i + 1, data[:i], nil
	}
	if atEOF && len(data) > 0 {
		return len(data), data, nil
	}
	return 0, nil, nil
}

// rejectConnection tells a client that has not completed the handshake why it is being turned away,
// then disables Anchor on that client so it does not keep reconnecting in a loop.
func rejectConnection(conn net.Conn, message string) {
	packet, _ := sjson.Set(`{"type":"SERVER_MESSAGE"}`, "message", message)
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	conn.Write(append([]byte(packet), 0))
	conn.Write(append([]byte(`{"type":"DISABLE_ANCHOR"}`), 0))
	time.Sleep(250 * time.Millisecond)
}
