package main

import (
	"encoding/json"
	"log"

	"github.com/google/uuid"
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

/*
Challenge #2: Unique ID Generation

Goal: generate unique IDs across a distributed system that can withstand network partitions.

UUIDs are a great solution for this because their size ensures uniqueness.
Since we don't need a ranking or ordering of IDs in our system, generating random UUIDs works fine.

We simply rely on their randomness to ensure uniqueness, so no communication between nodes is required.
*/
func main() {
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body into a map
		var body map[string]any

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Create the return message
		body["type"] = "generate_ok"
		body["id"] = uuid.New().String() // generate UUID

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
