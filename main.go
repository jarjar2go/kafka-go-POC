package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go" // go get github.com/segmentio/kafka-go
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

const (
	broker          = "localhost:9092"
	consumerGroupId = "consumer-group-id"
	topic           = "test"
)

func main() {
	// alternative: explicit commits: split message read in fetch and commit, committing marks fetched message as read
	// https://github.com/segmentio/kafka-go#explicit-commits
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{broker},
		GroupID:  consumerGroupId,
		Topic:    topic,
		MinBytes: 1, // default 0: waits for a few messages before reading
		MaxBytes: 1024,
	})

	// use message compression: https://github.com/segmentio/kafka-go#compression
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic:   topic,
	})

	go consume(reader, "A")
	go consume(reader, "B")
	go produce(writer, "X")

	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	<-exit

	if e := reader.Close(); e != nil { fmt.Println("Error closing reader:", e) }
	if e := writer.Close(); e != nil { fmt.Println("Error closing writer:", e) }
}

func consume(reader *kafka.Reader, name string) {
	for {
		m, err := reader.ReadMessage(context.Background())
		if err == io.EOF {
			return
		}

		fmt.Printf("<- %s %s Consumed at offset %d: %s = %s\n",
			time.Now().Format(time.StampMilli), name, m.Offset, m.Key, m.Value)
	}
}

func produce(writer *kafka.Writer, name string) {
	for i := 0; true; i++ {
		message := "Message " + strconv.Itoa(i)
		fmt.Printf("-> %s %s Producing: %s\n", time.Now().Format(time.StampMilli), name, message)

		err := writer.WriteMessages(context.Background(),
			kafka.Message{
				Value: []byte(message),
			},
		)
		if err == io.ErrClosedPipe {
			return
		}
	}
}
