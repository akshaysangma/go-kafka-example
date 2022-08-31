package consumer

import (
	"context"
	"math/rand"
	"time"

	"github.com/segmentio/kafka-go"
)

func Consume(ctx context.Context, r *kafka.Reader) {

	l := r.Config().Logger

	maxGoroutines := 10
	buffer := make(chan struct{}, maxGoroutines)

	for {
		buffer <- struct{}{}
		go func(
			ctx context.Context,
			l kafka.Logger,
			r *kafka.Reader,
			ch chan struct{}) {

			getMessageAsync(ctx, l, r)
			<-ch

		}(ctx, l, r, buffer)
	}
}

func getMessageAsync(
	ctx context.Context,
	l kafka.Logger,
	r *kafka.Reader) {

	processTimeSecondsMin := 30
	processTimeSecondsVariance := 10

	l.Printf("Reading..")

	// the `FetchMessage` method blocks until we receive the next event
	msg, err := r.FetchMessage(ctx)
	if err != nil {
		panic("could not read message " + err.Error())
	}

	// after receiving the message, log its value
	l.Printf("received: %v %v %v", string(msg.Value), msg.Partition, msg.Offset)

	// generate fake processing time
	rand.Seed(time.Now().UnixNano())
	processTimeSeconds := rand.Intn(processTimeSecondsVariance) + processTimeSecondsMin
	l.Printf("fake processing for %d Seconds...", processTimeSeconds)
	time.Sleep(time.Duration(processTimeSeconds) * time.Second)
	l.Printf("fake processing completed")

	//explicit commit
	l.Printf("Commiting message %v %v", msg.Partition, msg.Offset)
	if err := r.CommitMessages(ctx, msg); err != nil {
		l.Printf("failed to commit messages: %v", err)
		panic("commit failed")

	}
}
