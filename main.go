package main

import (
	"context"
	"log"
	"os"

	"github.com/akshaysangma/go-kafka-example/consumer"
	"github.com/segmentio/kafka-go"
)

func main() {

	l := log.New(os.Stdout, "Kakfa-Consumer", log.LstdFlags)
	// initialize a new reader with the brokers and topic
	// the groupID identifies the consumer and prevents
	// it from receiving duplicate messages
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{":9092"},
		Topic:       "ASx3",
		GroupID:     "my-group",
		Logger:      l,
		StartOffset: kafka.FirstOffset,
	})

	consumer.Consume(context.Background(), r)
}
