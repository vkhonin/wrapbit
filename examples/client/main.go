package main

import (
	"encoding/json"
	"fmt"
	"github.com/vkhonin/wrapbit"
	"math/rand"
	"os"
	"time"
)

type message struct {
	ID    uint64 `json:"id"`
	Value string `json:"value"`
}

func fatal(err error) {
	println(err.Error())
	os.Exit(1)
}

func main() {
	wrapbitInstance, err := wrapbit.NewInstance(
		wrapbit.WithNode("amqp://guest:guest@localhost:5673"),
		wrapbit.WithQueue("test_queue"),
	)
	if err != nil {
		fatal(err)
	}

	if err = wrapbitInstance.Start(); err != nil {
		fatal(err)
	}

	publisherInstance, err := wrapbitInstance.NewPublisher(
		"test_publisher",
		wrapbit.WithRoutingKey("test_queue"),
	)
	if err != nil {
		fatal(err)
	}

	if err = publisherInstance.Start(); err != nil {
		fatal(err)
	}

	consumerInstance, err := wrapbitInstance.NewConsumer("test_queue")
	if err != nil {
		fatal(err)
	}

	go func() {
		cErr := consumerInstance.Start(func(delivery *wrapbit.Delivery) (wrapbit.Response, error) {
			var m message

			if err = json.Unmarshal(delivery.Body(), &m); err != nil {
				return wrapbit.NackDiscard, fmt.Errorf("unmarshal: %w", err)
			}

			if rand.Intn(100) >= 50 {
				return wrapbit.NackRequeue, nil
			}

			fmt.Printf("message %d: %s\n", m.ID, m.Value)

			return wrapbit.Ack, nil
		})
		if cErr != nil {
			fatal(cErr)
		}
	}()

	for _, msg := range []message{{1, "hello"}, {2, "world"}} {
		data, _ := json.Marshal(msg)
		if err = publisherInstance.Publish(data); err != nil {
			fatal(err)
		}
	}

	time.Sleep(time.Second)

	if err = consumerInstance.Stop(); err != nil {
		fatal(err)
	}

	if err = publisherInstance.Stop(); err != nil {
		fatal(err)
	}

	if err = wrapbitInstance.Stop(); err != nil {
		fatal(err)
	}
}
