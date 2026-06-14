package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Allow all connections for live dashboard UI
	},
}

// Registry for all connected browsers
var clients = make(map[*websocket.Conn]bool)
var clientsMutex sync.Mutex

// listenAndBroadcast relays the Redis Pub/Sub array payload to WebSockets
func listenAndBroadcast(rdb *redis.Client) {
	ctx := context.Background()
	pubsub := rdb.Subscribe(ctx, "live-scores")
	defer pubsub.Close()

	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			log.Printf("Redis Receive Error: %v", err)
			continue
		}

		// Safely snapshot clients to avoid lock contention
		clientsMutex.Lock()
		activeClients := make([]*websocket.Conn, 0, len(clients))
		for client := range clients {
			activeClients = append(activeClients, client)
		}
		clientsMutex.Unlock()

		for _, client := range activeClients {
			client.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := client.WriteMessage(websocket.TextMessage, []byte(msg.Payload))
			client.SetWriteDeadline(time.Time{})

			if err != nil {
				client.Close()
				clientsMutex.Lock()
				delete(clients, client)
				clientsMutex.Unlock()
			}
		}
	}
}

// handleWebSocket registers new UI connections
func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	ws, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket Upgrade Error: %v", err)
		return
	}

	defer func() {
		clientsMutex.Lock()
		delete(clients, ws)
		clientsMutex.Unlock()
		ws.Close()
	}()

	clientsMutex.Lock()
	clients[ws] = true
	clientsMutex.Unlock()
	fmt.Println("New browser connected to live feed!")

	for {
		if _, _, err := ws.ReadMessage(); err != nil {
			break // Client disconnected cleanly
		}
	}
}
