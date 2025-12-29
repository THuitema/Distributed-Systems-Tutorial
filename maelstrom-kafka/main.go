package main

/*
Challenge #5: Kafka-Style Log

Goal: implement a replicated log service similar to Kafka

Part a) Single-node log system
Part b) Distributed log system utilizing a linearizable key-value service
*/

import (
	"encoding/json"
	"log"
	"context"
	"errors"
	"fmt"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequestBody struct {
	Type    string `json:"type"`
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type SendResponseBody struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type PollRequestBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponseBody struct {
	Type     string             `json:"type"`
	Messages map[string][][]int `json:"msgs"`
}

type CommitOffsetsRequestBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type CommitOffsetsResponseBody struct {
	Type string `json:"type"`
}

type ListCommittedOffsetsRequestBody struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type ListCommittedOffsetsResponseBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

func main() {
	node := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(node)
	ctx := context.Background()

	node.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		var offset int

		// Step 1: Retrieve highest offset for this key (-1 if it doesn't exist) then increment it
		offsetKey := fmt.Sprintf("%s/highest_offset", body.Key)
		for {
			oldOffset, err := kv.ReadInt(ctx, offsetKey)

			if err != nil {
				// If key doesn't exist oldValue is 0
				var rpcErr *maelstrom.RPCError
				if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
					oldOffset = -1
				} else {
					return err
				}
			}

			// Increment old offset by 1
			err = kv.CompareAndSwap(ctx, offsetKey, oldOffset, oldOffset + 1, true)

			if err == nil {
				offset = oldOffset + 1
				break
			}
		}

		// Step 2: Write the message as a new key-value pair with the updated offset
		logEntryKey := fmt.Sprintf("%s/data/%d", body.Key, offset)

		err := kv.Write(ctx, logEntryKey, body.Message)

		if err != nil {
			return err
		}

		return node.Reply(msg, SendResponseBody{
			Type: "send_ok",
			Offset: offset,
		})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		var body PollRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := make(map[string][][]int)

		// Collect messages starting from given offset for each log
		for key, startOffset := range body.Offsets {
			logMessages := [][]int{}

			// Read highest offset for key
			// Ignore this loop iteration if key doesnt exist
			offsetKey := fmt.Sprintf("%s/highest_offset", key)

			highestOffset, err := kv.ReadInt(ctx, offsetKey)

			if err == nil {
				count := 0

				// Iterate through all keys in offset range (startOffset, highestOffset), appending up to three to list
				for i := startOffset; i <= highestOffset; i++ {
					logEntryKey := fmt.Sprintf("%s/data/%d", key, i)

					val, err := kv.Read(ctx, logEntryKey)

					if err == nil {
						if msg, ok := val.(int); ok {
							logMessages = append(logMessages, []int{i, msg})
							count += 1

							if count == 3 {
								break
							}
						}
					}
				}
			}
		
			messages[key] = logMessages
		} 

		// Collect up to three messages 

		return node.Reply(msg, PollResponseBody{
			Type: "poll_ok",
			Messages: messages,
		})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body CommitOffsetsRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Set the committed offset for each key, if it exists
		// Keep old committed offset if it is greater than the new offset
		for key, newOffset := range body.Offsets {
			committedOffsetKey := fmt.Sprintf("%s/committed_offset", key)

			for {
				oldCommittedOffset, err := kv.ReadInt(ctx, committedOffsetKey)

				if err != nil {
					var rpcErr *maelstrom.RPCError
					if errors.As(err, &rpcErr) && rpcErr.Code == maelstrom.KeyDoesNotExist {
						oldCommittedOffset = 0
					} else {
						return err
					}
				}

				// New committed offset is greater of old and new
				err = kv.CompareAndSwap(ctx, committedOffsetKey, oldCommittedOffset, max(oldCommittedOffset, newOffset), true)

				if err == nil {
					break
				}
			}
		}

		return node.Reply(msg, CommitOffsetsResponseBody{
			Type: "commit_offsets_ok",
		})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body ListCommittedOffsetsRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		// Extract committed offset from each given key, if it exists
		for _, key := range body.Keys {
			committedOffsetKey := fmt.Sprintf("%s/committed_offset", key)
			committedOffset, err := kv.ReadInt(ctx, committedOffsetKey)

			if err == nil {
				offsets[key] = committedOffset
			}
		}

		return node.Reply(msg, ListCommittedOffsetsResponseBody{
			Type: "list_committed_offsets_ok",
			Offsets: offsets,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}