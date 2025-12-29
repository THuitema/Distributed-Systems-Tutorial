package main

/*
Challenge #5: Kafka-Style Log

Goal: implement a replicated log service similar to Kafka

Part a) Single-node log system
*/

import (
	"encoding/json"
	"log"
	"sync"

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

type ThreadSafeLog struct {
	mu   sync.Mutex
	logs map[string]*Log
}

type Log struct {
	CommittedOffset int
	LogEntries      []LogEntry
}

type LogEntry struct {
	Offset  int
	Message int
}

func main() {
	node := maelstrom.NewNode()
	safeLogs := ThreadSafeLog{logs: make(map[string]*Log)}

	node.Handle("send", func(msg maelstrom.Message) error {
		safeLogs.mu.Lock()
		defer safeLogs.mu.Unlock()

		var body SendRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		
		var offset int

		// Append entry to existing log, otherwise create new log
		if log, ok := safeLogs.logs[body.Key]; ok {
			offset = log.LogEntries[len(log.LogEntries)-1].Offset + 1 // 1 higher than prev offset
			log.LogEntries = append(log.LogEntries, LogEntry{
				Offset: offset, 
				Message: body.Message,
			})
		} else {
			offset = (len(safeLogs.logs) + 1) * 1000
			safeLogs.logs[body.Key] = &Log{
				CommittedOffset: offset, // TODO: might need to be offset - 1
				LogEntries: []LogEntry{
					{
						Offset: offset,
						Message: body.Message,
					},
				},
			}
		}

		return node.Reply(msg, SendResponseBody{
			Type: "send_ok",
			Offset: offset,
		})
	})

	node.Handle("poll", func(msg maelstrom.Message) error {
		safeLogs.mu.Lock()
		defer safeLogs.mu.Unlock()

		var body PollRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		messages := make(map[string][][]int)

		// Collect messages starting from given offset for each log
		for key, offset := range body.Offsets {
			logMessages := [][]int{}

			if log, ok := safeLogs.logs[key]; ok {
				count := 0

				// Collect up to 3 messages from log starting from given offset
				for _, entry := range log.LogEntries {
					if entry.Offset >= offset {
						logMessages = append(logMessages, []int{entry.Offset, entry.Message})

						count += 1
						if count == 3 {
							break
						}
					}
				}
			}

			messages[key] = logMessages
		} 

		return node.Reply(msg, PollResponseBody{
			Type: "poll_ok",
			Messages: messages,
		})
	})

	node.Handle("commit_offsets", func(msg maelstrom.Message) error {
		safeLogs.mu.Lock()
		defer safeLogs.mu.Unlock()

		var body CommitOffsetsRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Set the committed offset for each key, if it exists
		// Keep old committed offset if it is greater than the new offset
		for key, offset := range body.Offsets {
			if log, ok := safeLogs.logs[key]; ok {
				log.CommittedOffset = max(log.CommittedOffset, offset) // TODO: idk if you need to remember the old committed offset
			}
		}

		return node.Reply(msg, CommitOffsetsResponseBody{
			Type: "commit_offsets_ok",
		})
	})

	node.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		safeLogs.mu.Lock()
		defer safeLogs.mu.Unlock()

		var body ListCommittedOffsetsRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		offsets := make(map[string]int)

		// Extract committed offset from each given key, if it exists
		for _, key := range body.Keys {
			if log, ok := safeLogs.logs[key]; ok {
				offsets[key] = log.CommittedOffset
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