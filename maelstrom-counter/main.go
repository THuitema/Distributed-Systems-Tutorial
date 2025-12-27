package main

/*
Challenge #4: Grow-Only Counter

Goal: implement a stateless, sequentially-consistent global counter 
*/

import (
	"context"
	"encoding/json"
	"log"
	"time"
	"errors"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type AddRequestBody struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

type AddResponseBody struct {
	Type string `json:"type"`
}

type ReadRequestBody struct {
	Type string `json:"type"`
}

type ReadResponseBody struct {
	Type  string `json:"type"`
	Value int    `json:"value"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	ctx := context.Background()

	n.Handle("add", func(msg maelstrom.Message) error {
		var body AddRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Attempt to write new value while preventing race conditions
		for {
			// Read current value
			oldValue, err := kv.ReadInt(ctx, "global_total")

			if err != nil {
				// If key doesn't exist oldValue is 0
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					oldValue = 0
				} else {
					return err
				}
			}

			// Try to write new value (create key if doesn't exist)
			err = kv.CompareAndSwap(ctx, "global_total", oldValue, oldValue + body.Delta, true)

			if err == nil {
				break
			}

			time.Sleep(time.Millisecond * 100) // to prevent many reads
		}

		return n.Reply(msg, AddResponseBody{
			Type: "add_ok",
		})
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		var body ReadRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		value, err := kv.ReadInt(ctx, "global_total")

		if err != nil {
			// If key doesn't exist, value is 0
			var rpcErr *maelstrom.RPCError
    		if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
				value = 0
			} else {
				return err
			}
		}

		return n.Reply(msg, ReadResponseBody{
			Type: "read_ok",
			Value: value,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
} 