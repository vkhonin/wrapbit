package wrapbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	Ack Response = iota
	NackDiscard
	NackRequeue
)

type Consumer struct {
	channel         *amqp.Channel
	closeChannel    <-chan *amqp.Error
	config          ConsumerConfig
	deliveryChannel <-chan amqp.Delivery
	errorChannel    chan error
	wrapbit         *Wrapbit
}

type ConsumerConfig struct {
	args          amqp.Table
	autoAck       bool
	autoReconnect bool
	consumer      string
	exclusive     bool
	noLocal       bool
	noWait        bool
	queue         string
}

type ConsumerOption func(p *Consumer) error

// Handler should contain Delivery-handling logic and return Response code and error if any. In case consumer is
// autoAck, Response code is not honoured.
type Handler func(delivery *Delivery) (Response, error)

type Delivery struct {
	delivery *amqp.Delivery
}

type Response uint8

func consumerDefaultConfig() ConsumerConfig {
	return ConsumerConfig{
		args:          nil,
		autoAck:       false,
		autoReconnect: false,
		consumer:      "",
		exclusive:     false,
		noLocal:       false,
		noWait:        false,
		queue:         "",
	}
}

// WithAutoReconnect enables automatic reconnection on consumer channel error
func WithAutoReconnect() ConsumerOption {
	return func(c *Consumer) error {
		c.config.autoReconnect = true

		return nil
	}
}

func (c *Consumer) Start(handler Handler) error {
	ch, err := c.wrapbit.newChannel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	c.channel = ch

	c.deliveryChannel, err = c.channel.Consume(
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

	c.closeChannel = c.channel.NotifyClose(make(chan *amqp.Error))

	go c.consume(handler)

	return nil
}

func (c *Consumer) Stop() error {
	if c.channel == nil {
		return nil
	}

	return c.channel.Close()
}

func (c *Consumer) consume(handler Handler) {
	for c.deliveryChannel != nil || c.closeChannel != nil {
		select {
		case d, ok := <-c.deliveryChannel:
			if !ok {
				c.deliveryChannel = nil

				break
			}
			dd := Delivery{
				delivery: &d,
			}
			result, err := handler(&dd)
			if err != nil {
				c.wrapbit.logger.Error(fmt.Sprintf("handling Delivery: %v", err))

				break
			}
			switch result {
			case Ack:
				if err = dd.delivery.Ack(false); err != nil {
					c.wrapbit.logger.Error(fmt.Sprintf("ack: %v", err))
				}
			case NackDiscard:
				if err = dd.delivery.Nack(false, false); err != nil {
					c.wrapbit.logger.Error(fmt.Sprintf("nack discard: %v", err))
				}
			case NackRequeue:
				if err = dd.delivery.Nack(false, true); err != nil {
					c.wrapbit.logger.Error(fmt.Sprintf("nack requeue: %v", err))
				}
			}
		case closeErr := <-c.closeChannel:
			if c.config.autoReconnect && closeErr != nil {
				// TODO: This could be infinite loop. Some break condition (and probably timeout) required.
				for {
					if startErr := c.Start(handler); startErr == nil {
						break
					}
				}
			}

			return
		}
	}
}

func (d *Delivery) Body() []byte {
	return d.delivery.Body
}
