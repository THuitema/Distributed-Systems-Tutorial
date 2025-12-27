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

		go func(adjacencies []string, body BroadcastRequestBody) {
			var deliveredNodes []string

			for len(deliveredNodes) < len(adjacencies) {
				for _, adjNode := range adjacencies {
					if !slices.Contains(deliveredNodes, adjNode) {

						// Use RPC since we expect a broadcast_ok
						n.RPC(adjNode, body, func(msg maelstrom.Message) error {
							var broadcastOkBody BroadcastOkBody

							if err := json.Unmarshal(msg.Body, &broadcastOkBody); err != nil {
								return err
							}

							if broadcastOkBody.Type != "broadcast_ok" {
								return fmt.Errorf("expected type broadcast_ok, got %s", broadcastOkBody.Type)
							}

							// Mark node as delivered
							deliveredNodes = append(deliveredNodes, adjNode)
							return nil
						})
					}
				}

				// Wait between repeated broadcast attempts to the same node
				time.Sleep(time.Second)
			}
		}(destinations, body)

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
