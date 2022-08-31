package main

import (
	"context"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// to produce messages
	topic := "ASx3"
	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:9092", "localhost:9093", "localhost:9094"),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		}, kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Second A"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("Second B"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Second C"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
