package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type Room struct {
	id      string
	clients sync.Map
	teams   sync.Map
	state   string      // Room Settings
	mu      sync.Mutex  // Mutex for safely updating state
	dirty   atomic.Bool // Session changed since it was last written to disk
}

func NewRoom(id string, ownerClientId uint64, packet string) *Room {
	roomState, _ := sjson.Set(gjson.Get(packet, "roomState").Raw, "ownerClientId", ownerClientId)

	room := &Room{
		id:      id,
		clients: sync.Map{},
		teams:   sync.Map{},
		state:   roomState,
	}
	room.markDirty()
	return room
}

func (r *Room) findOrCreateTeam(teamId string) *Team {
	value, ok := r.teams.Load(teamId)
	if !ok {
		value, _ = r.teams.LoadOrStore(teamId, &Team{
			id:    teamId,
			state: "{}",
			room:  r,
			queue: make([]string, 0),
		})
	}

	return value.(*Team)
}

func (r *Room) broadcastPacket(packet string) {
	clientId := gjson.Get(packet, "clientId").Uint()

	r.clients.Range(func(_, value interface{}) bool {
		client := value.(*Client)
		if client.id != clientId {
			client.sendPacket(packet)
		}

		return true
	})
}

func (r *Room) broadcastAllClientState() {
	r.mu.Lock()
	defer r.mu.Unlock()

	packet := `{"type":"ALL_CLIENT_STATE","state":[]}`

	idToIndex := make(map[interface{}]int)
	index := 0

	r.clients.Range(func(id, value interface{}) bool {
		client := value.(*Client)
		idToIndex[id] = index
		client.mu.Lock()
		packet, _ = sjson.SetRaw(packet, "state."+fmt.Sprint(index), client.state)
		client.mu.Unlock()
		index++
		return true
	})

	r.clients.Range(func(id, value interface{}) bool {
		index, ok := idToIndex[id]
		if !ok {
			// Joined while this packet was being put together, so it isn't in it (and index 0 would mark someone
			// else as "self"). Its own join sends everyone a newer packet anyway.
			return true
		}
		client := value.(*Client)
		clientPacket, _ := sjson.Set(packet, "state."+fmt.Sprint(index)+".self", true)
		client.sendPacket(clientPacket)
		return true
	})
}

// pongPacket answers a ping: the time the client sent (so it can measure its ping) and the ping and hearts of
// everyone online in the room, as {"<clientId>":[pingMs, health, healthCapacity]}
func (r *Room) pongPacket(t string) string {
	packet := `{"type":"PONG","quiet":true,"clients":{}}`
	if t != "" {
		packet, _ = sjson.SetRaw(packet, "t", t)
	}

	r.clients.Range(func(_, value interface{}) bool {
		client := value.(*Client)
		client.mu.Lock()
		online := client.conn != nil
		entry := fmt.Sprintf("[%d,%d,%d]", client.rtt, client.hp, client.maxHp)
		client.mu.Unlock()
		if online {
			// ":" makes sjson use the number as an object key instead of an array index
			packet, _ = sjson.SetRaw(packet, "clients.:"+fmt.Sprint(client.id), entry)
		}
		return true
	})

	return packet
}

func (r *Room) GetLastActivity() time.Time {
	var lastActivity time.Time

	r.clients.Range(func(id, value interface{}) bool {
		client := value.(*Client)
		client.mu.Lock()
		if client.lastActivity.After(lastActivity) {
			lastActivity = client.lastActivity
		}
		client.mu.Unlock()
		return true
	})

	return lastActivity
}
