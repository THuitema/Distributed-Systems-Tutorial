package main

import (
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"slices"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
Challenge #3: Broadcast

Benchmarks:

Grid Topology:
  Messages-per-op: 53.56
  Median Latency: 451 ms
  Max Latency: 799 ms

Line Topology:
  Messages-per-op: 23.56
  Median Latency: 1572
  Max Latency: 2433
*/

// Broadcast RPC
type BroadcastRequestBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type BroadcastOkBody struct {
	Type      string `json:"type"`
	InReplyTo int    `json:"in_reply_to"`
}

type BroadcastResponseBody struct {
	Type string `json:"type"`
}

// Read RPC
type ReadRequestBody struct {
	Type string `json:"type"`
}

type ReadResponseBody struct {
	Type     string `json:"type"`
	Messages []int  `json:"messages"`
}

// Topology RPC
type TopologyRequestBody struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

type TopologyResponseBody struct {
	Type string `json:"type"`
}

type SafeMessageMap struct {
	mu sync.Mutex
	v  map[int]bool
}

func main() {
	n := maelstrom.NewNode()
	var destinations []string

	messageMap := SafeMessageMap{v: make(map[int]bool)}

	// This message requests that a value be broadcast out to all nodes in the cluster
	// Always an integer and unique
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body into a map
		var body BroadcastRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Stop broadcasting if we already received message
		if messageMap.Exists(body.Message) {
			return n.Reply(msg, BroadcastResponseBody{
				Type: "broadcast_ok",
			})
		}

		// what if we make a thread for each attempted broadcast
		for _, adjNode := range destinations {
			if adjNode != msg.Src {
				go func(adjNode string, body BroadcastRequestBody) {
					delivered := false

					for !delivered {
						n.RPC(adjNode, body, func(msg maelstrom.Message) error {
							var broadcastOkBody BroadcastOkBody

							if err := json.Unmarshal(msg.Body, &broadcastOkBody); err != nil {
								return err
							}

							if broadcastOkBody.Type != "broadcast_ok" {
								return fmt.Errorf("expected type broadcast_ok, got %s", broadcastOkBody.Type)
							}

							// Mark node as delivered
							delivered = true
							return nil
						})

						time.Sleep(time.Second)
					}
				}(adjNode, body)
			}
		}

		return n.Reply(msg, BroadcastResponseBody{
			Type: "broadcast_ok",
		})
	})

	n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
		return nil
	})

	// This message requests that a node return all values it has seen
	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var body ReadRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		keys := messageMap.KeyList()

		return n.Reply(msg, ReadResponseBody{
			Type:     "read_ok",
			Messages: keys,
		})
	})

	// This message informs the node of who its neighboring nodes are
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body into a map
		var body TopologyRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		destinations = body.Topology[n.ID()]
		_ = destinations

		log.Print("Topology received!")

		return n.Reply(msg, TopologyResponseBody{
			Type: "topology_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}

// Returns true if we have already received the given message
// If message doesn't exist, we add it to our list
// Locks so only one goroutine can access the map
func (c *SafeMessageMap) Exists(message int) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, exists := c.v[message]

	if !exists {
		c.v[message] = true
	}

	return exists
}

// Returns list of keys in messageMap
func (c *SafeMessageMap) KeyList() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Collect(maps.Keys(c.v))
}


