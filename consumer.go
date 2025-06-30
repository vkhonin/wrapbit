package wrapbit

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
)

const (
	Ack Response = iota
	NackDiscard
	NackRequeue
)

type Consumer struct {
	channel *amqp091.Channel
	config  ConsumerConfig
	wrapbit *Wrapbit
}

type ConsumerConfig struct {
	args      amqp091.Table
	autoAck   bool
	consumer  string
	exclusive bool
	noLocal   bool
	noWait    bool
	queue     string
}

type ConsumerOption func(p *Consumer) error

// Handler should contain Delivery-handling logic and return Response code and error if any. In case consumer is
// autoAck, Response code is not honoured.
type Handler func(delivery *Delivery) (Response, error)

type Delivery struct {
	delivery *amqp091.Delivery
}

type Response uint8

func (c *Consumer) Start(handler Handler) error {
	ch, err := c.wrapbit.connection.Channel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	c.channel = ch

	dCh, err := c.channel.Consume(
		c.config.queue,
		c.config.consumer,
		c.config.autoAck,
		c.config.exclusive,
		c.config.noLocal,
		c.config.noWait,
		c.config.args,
	)
	if err != nil {
		return fmt.Errorf("start consume: %w", err)
	}

	for {
		select {
		case d, ok := <-dCh:
			if !ok {
				return nil
			}
			dd := Delivery{
				delivery: &d,
			}
			result, err := handler(&dd)
			if err != nil {
				return fmt.Errorf("handling Delivery: %w", err)
			}
			switch result {
			case Ack:
				if err = dd.delivery.Ack(false); err != nil {
					return fmt.Errorf("ack: %w", err)
				}
			case NackDiscard:
				if err = dd.delivery.Nack(false, false); err != nil {
					return fmt.Errorf("nack discard: %w", err)
				}
			case NackRequeue:
				if err = dd.delivery.Nack(false, true); err != nil {
					return fmt.Errorf("nack requeue: %w", err)
				}
			}
		}
	}
}

func (c *Consumer) Stop() error {
	return c.channel.Close()
}

func (d *Delivery) Body() []byte {
	return d.delivery.Body
}
