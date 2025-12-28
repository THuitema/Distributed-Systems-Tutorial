package main

/*
Challenge #5: Kafka-Style Log

Goal: implement a replicated log service similar to Kafka

Part a) Single-node log system
*/

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type SendRequestBody struct {
	Type    string `json:"type"`
	Key     string `json:"key"`
	Message int    `json:"msg"`
}

type SendResponseBody struct {
	Type   string `json:"type"`
	Offset int    `json:"msg"`
}

type PollRequestBody struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type PollResponseBody struct {
	Type     string             `json:"type"`
	Messages map[string][][]int `json:"msgs"`
}

type LogEntry struct {
	Offset  int
	Message int
}

func main() {
	node := maelstrom.NewNode()

	logs := make(map[string][]LogEntry)

	node.Handle("send", func(msg maelstrom.Message) error {
		var body SendRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		entries, ok := logs[body.Key]
		var offset int

		// Append entry to existing log, otherwise create new log
		if ok {
			offset = entries[len(entries) - 1].Offset + 1 // 1 higher than prev offset
			entries = append(entries, LogEntry{
				Offset: offset, 
				Message: body.Message,
			})
		} else {
			offset = (len(logs) + 1) * 1000
			logs[body.Key] = []LogEntry{
				{
					Offset: offset,
					Message: body.Message,
				},
			}
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
		for key, offset := range body.Offsets {
			var logMessages [][]int

			_, ok := logs[key]

			if ok {
				count := 0
				// Collect up to 3 messages from log starting from given offset
				for _, entry := range logs[key] {
					if entry.Offset >= offset {
						logMessages = append(logMessages, []int{entry.Offset, entry.Message})

						count += 1
						if count == 2 {
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

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}