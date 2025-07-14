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
	channel         *channel
	closeChannel    <-chan *amqp.Error
	config          ConsumerConfig
	deliveryChannel <-chan amqp.Delivery
	errorChannel    chan error
	logger          Logger
}

type ConsumerConfig struct {
	args          amqp.Table
	autoAck       bool
	autoReconnect bool
	consumer      string
	exclusive     bool
	noLocal       bool
	noWait        bool
	prefetchCount int
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
		prefetchCount: 1,
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

// WithPrefetchCount sets maximum number of deliveries sent by server without acknowledgement
func WithPrefetchCount(n int) ConsumerOption {
	return func(c *Consumer) error {
		c.config.prefetchCount = n

		return nil
	}
}

func (c *Consumer) Start(handler Handler) error {
	var err error

	c.logger.Debug("Setting up consumer.")

	if err = c.channel.connect(); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	c.logger.Debug("Declaring QoS.")

	if err = c.channel.channel.Qos(c.config.prefetchCount, 0, false); err != nil {
		return fmt.Errorf("setting QoS: %w", err)
	}

	c.logger.Debug("Declaring consume.")

	c.deliveryChannel, err = c.channel.channel.Consume(
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

	c.logger.Debug("Setting up channel notifications.")

	c.closeChannel = c.channel.channel.NotifyClose(make(chan *amqp.Error))

	c.logger.Debug("Start consuming.")

	go c.consume(handler)

	c.logger.Debug("Consumer set up.")

	return nil
}

func (c *Consumer) Stop() error {
	c.logger.Debug("Stopping publisher.")

	if err := c.channel.disconnect(); err != nil {
		return err
	}

	c.logger.Debug("Publisher stopped.")

	return nil
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
				c.logger.Error("Delivery handler error.", err)

				break
			}
			switch result {
			case Ack:
				if err = dd.delivery.Ack(false); err != nil {
					c.logger.Error("Ack error.", err)
				}
			case NackDiscard:
				if err = dd.delivery.Nack(false, false); err != nil {
					c.logger.Error("Nack discard error.", err)
				}
			case NackRequeue:
				if err = dd.delivery.Nack(false, true); err != nil {
					c.logger.Error("Nack requeue error.", err)
				}
			}
		case closeErr := <-c.closeChannel:
			if closeErr != nil {
				c.logger.Warn("Consumer channel error.", closeErr)

				if c.config.autoReconnect {
					// TODO: This could be infinite loop. Some break condition (and probably timeout) required.
					for {
						if startErr := c.Start(handler); startErr == nil {
							c.logger.Warn("Consumer restart error.", startErr)

							break
						}
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
