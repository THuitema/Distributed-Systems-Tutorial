package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
Challenge #3a
*/

// Broadcast RPC
type BroadcastRequest struct {
	Type    string  `json:"type"`
	Message int     `json:"message"`
}

// Topology RPC
type TopologyRequest struct {
	Type     string              `json:"type"`
	Topology map[string][]string `json:"topology"`
}

func main() {
	n := maelstrom.NewNode()
	var messages []int
	var topology map[string][]string


	// This message requests that a value be broadcast out to all nodes in the cluster
	// Always an integer and unique
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// Unmarshal the message body into a map
		var body BroadcastRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages = append(messages, body.Message)

		responseBody := make(map[string]any)
		responseBody["type"] = "broadcast_ok"

		return n.Reply(msg, responseBody)
	})

	// This message requests that a node return all values it has seen
	n.Handle("read", func(msg maelstrom.Message) error {
		responseBody := make(map[string]any)

		responseBody["type"] = "read_ok"
		responseBody["messages"] = messages
		return n.Reply(msg, responseBody)
	})

	// This message informs the node of who its neighboring nodes are
	n.Handle("topology", func(msg maelstrom.Message) error {
		// Unmarshal the message body into a map
		var body TopologyRequest

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topology = body.Topology
		_ = topology // just so it compiles for now. remove when we actually read topology

		responseBody := make(map[string]any)
		responseBody["type"] = "topology_ok"
		return n.Reply(msg, responseBody)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
