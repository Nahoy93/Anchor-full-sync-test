package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"time"
)

// Session persistence
//
// Every room (session) is saved to sessions/<room>.json: the room settings plus, for every team, the last save state
// a player pushed and the queue of items/flags collected since. When the server restarts, or the room was unloaded
// after being inactive, the session is loaded again as soon as someone joins the same Room ID. Players then continue
// from their own save file and get any progress their team made in the meantime.
//
// Different Room IDs are different sessions, so you can have as many as you like side by side.

const SESSIONS_DIR = "sessions"
const SAVE_INTERVAL = 10 * time.Second

type savedTeam struct {
	State json.RawMessage `json:"state"`
	Queue []string        `json:"queue"`
}

type savedSession struct {
	RoomId    string               `json:"roomId"`
	RoomState json.RawMessage      `json:"roomState"`
	Teams     map[string]savedTeam `json:"teams"`
	SavedAt   string               `json:"savedAt"`
}

var unsafeChars = regexp.MustCompile(`[^A-Za-z0-9_-]`)

// The file name keeps a readable part of the room id plus a hash, so different ids never collide and the room id
// (which works like a password for public servers) isn't the whole file name
func sessionPath(roomId string) string {
	sum := sha1.Sum([]byte(roomId))
	readable := unsafeChars.ReplaceAllString(roomId, "_")
	if len(readable) > 32 {
		readable = readable[:32]
	}
	return filepath.Join(SESSIONS_DIR, readable+"-"+hex.EncodeToString(sum[:4])+".json")
}

func isPersistedRoom(roomId string) bool {
	return roomId != "" && roomId != "soh-global"
}

func (r *Room) markDirty() {
	r.dirty.Store(true)
}

func (r *Room) save() error {
	if !isPersistedRoom(r.id) {
		return nil
	}

	r.mu.Lock()
	session := savedSession{
		RoomId:    r.id,
		RoomState: json.RawMessage(r.state),
		Teams:     map[string]savedTeam{},
		SavedAt:   time.Now().Format(time.RFC3339),
	}
	r.mu.Unlock()

	r.teams.Range(func(key, value interface{}) bool {
		team := value.(*Team)
		team.mu.Lock()
		queue := make([]string, len(team.queue))
		copy(queue, team.queue)
		state := team.state
		team.mu.Unlock()
		if state == "" {
			state = "{}"
		}
		session.Teams[key.(string)] = savedTeam{State: json.RawMessage(state), Queue: queue}
		return true
	})

	data, err := json.Marshal(session)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(SESSIONS_DIR, 0755); err != nil {
		return err
	}

	// Write to a temp file first so a crash mid-write never leaves a broken session behind
	path := sessionPath(r.id)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// loadSession returns a room restored from disk, or nil if there is no saved session for this id
func loadSession(roomId string) *Room {
	if !isPersistedRoom(roomId) {
		return nil
	}

	data, err := os.ReadFile(sessionPath(roomId))
	if err != nil {
		return nil
	}

	var session savedSession
	if err := json.Unmarshal(data, &session); err != nil {
		log.Printf("Session file for room %s is broken, starting fresh: %v", roomId, err)
		return nil
	}

	room := &Room{id: roomId, state: string(session.RoomState)}
	for teamId, saved := range session.Teams {
		queue := saved.Queue
		if queue == nil {
			queue = []string{}
		}
		state := string(saved.State)
		if state == "" || state == "null" {
			state = "{}"
		}
		room.teams.Store(teamId, &Team{id: teamId, state: state, room: room, queue: queue})
	}

	log.Printf("Session %s restored (saved %s, %d teams)", roomId, session.SavedAt, len(session.Teams))
	return room
}

func (s *Server) saveDirtySessions(errChan chan error) {
	ticker := time.NewTicker(SAVE_INTERVAL)
	defer ticker.Stop()
	defer func() {
		if r := recover(); r != nil {
			errChan <- fmt.Errorf("panic in saveDirtySessions: %v", r)
		}
	}()

	for range ticker.C {
		s.saveAllSessions(false)
	}
}

func (s *Server) saveAllSessions(force bool) {
	s.rooms.Range(func(_, value interface{}) bool {
		room := value.(*Room)
		if force || room.dirty.Swap(false) {
			if err := room.save(); err != nil {
				log.Printf("Could not save session %s: %v", room.id, err)
			}
		}
		return true
	})
}

type sessionInfo struct {
	RoomId  string
	SavedAt string
	Teams   int
	File    string
}

func listSessions() []sessionInfo {
	files, _ := filepath.Glob(filepath.Join(SESSIONS_DIR, "*.json"))
	var result []sessionInfo
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			continue
		}
		var session savedSession
		if json.Unmarshal(data, &session) != nil {
			continue
		}
		result = append(result, sessionInfo{session.RoomId, session.SavedAt, len(session.Teams), file})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].SavedAt > result[j].SavedAt })
	return result
}

func deleteSession(roomId string) bool {
	return os.Remove(sessionPath(roomId)) == nil
}
