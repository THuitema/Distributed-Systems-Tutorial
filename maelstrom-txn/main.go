package main

/*
Challenge #6: Totally-Available Transactions

Goal: implement a key/value store which implements transactions

Part a) Single-node system
*/

import (
	"encoding/json"
	// "errors"
	// "fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type TransactionRequestBody struct {
	Type        string  `json:"type"`
	Transaction [][]any `json:"txn"`
}

type TransactionResponseBody struct {
	Type        string  `json:"type"`
	Transaction [][]any `json:"txn"`
}

type KeyValueStore struct {
	mu sync.Mutex
	kv map[int]int
}

func main() {
	node := maelstrom.NewNode()
	store := KeyValueStore{kv: make(map[int]int)}

	node.Handle("txn", func(msg maelstrom.Message) error {
		store.mu.Lock()
		defer store.mu.Unlock()

		var body TransactionRequestBody

		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		transactionResult := [][]any{}

		for _, txn := range body.Transaction {
			var lookupKey int

			if f, ok := txn[1].(float64); ok {
				lookupKey = int(f)

				if txn[0] == "r" {
					if val, exists := store.kv[lookupKey]; exists {
						// Key exists, fetch value
						txn[2] = val
					}
					

				} else if txn[0] == "w" {
					var writeValue int

					if f, ok := txn[2].(float64); ok {
						writeValue = int(f)
						store.kv[lookupKey] = writeValue
					}
				}
				transactionResult = append(transactionResult, txn)
			}
		}

		return node.Reply(msg, TransactionResponseBody{
			Type: "txn_ok",
			Transaction: transactionResult,
		})
	})

	if err := node.Run(); err != nil {
		log.Fatal(err)
	}
}