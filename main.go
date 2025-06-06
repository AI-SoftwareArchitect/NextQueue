package main

import (
	"encoding/json"
	"net/http"
	"sort"
	"sync"
	"time"
)

type Message struct {
	Topic       string `json:"topic"`
	Message     string `json:"message"`
	MsgVersion  string `json:"msg_version"`
	Priority    int    `json:"priority"`
	Timestamp   int64  `json:"timestamp"`
}

type Queue struct {
	messages map[string][]Message
	mu       sync.RWMutex
}

func NewQueue() *Queue {
	return &Queue{
		messages: make(map[string][]Message),
	}
}

func (q *Queue) Produce(msg Message) {
	q.mu.Lock()
	defer q.mu.Unlock()
	msg.Timestamp = time.Now().UnixNano()
	q.messages[msg.Topic] = append(q.messages[msg.Topic], msg)

	sort.Slice(q.messages[msg.Topic], func(i, j int) bool {
		if q.messages[msg.Topic][i].Priority == q.messages[msg.Topic][j].Priority {
			return q.messages[msg.Topic][i].Timestamp < q.messages[msg.Topic][j].Timestamp
		}
		return q.messages[msg.Topic][i].Priority > q.messages[msg.Topic][j].Priority
	})
}

func (q *Queue) Consume(topic string) []Message {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.messages[topic]
}

var queue = NewQueue()

func produceHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	err := json.NewDecoder(r.Body).Decode(&msg)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	queue.Produce(msg)
	w.WriteHeader(http.StatusCreated)
}

func consumeHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic string `json:"topic"`
	}
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	messages := queue.Consume(req.Topic)
	json.NewEncoder(w).Encode(messages)
}

func main() {
	http.HandleFunc("/api/produce", produceHandler)
	http.HandleFunc("/api/consume", consumeHandler)
	http.ListenAndServe(":8888", nil)
}