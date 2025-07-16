package wrapbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/internal/transport"
	"github.com/vkhonin/wrapbit/utils"
)

const (
	Ack         Response = iota // ACK will be sent on handled Delivery.
	NackDiscard                 // NACK will be sent on handled Delivery. Message will be discarded without requeue.
	NackRequeue                 // NACK will be sent on handled Delivery. Message will be requeued.
)

// Consumer is an instance that consumes from its designated queue.
type Consumer struct {
	channel         *transport.Channel
	closeChannel    <-chan *amqp.Error
	config          consumerConfig
	deliveryChannel <-chan amqp.Delivery
	errorChannel    chan error
	logger          utils.Logger
}

type consumerConfig struct {
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

// Handler is a function supplied to [Consumer] and used to handle [Delivery]. It should return [Response] depending on
// desired acknowledgement type. If [Consumer] started with auto acknowledgement, then [Response] is not honored.
type Handler func(delivery *Delivery) (Response, error)

// Delivery is an entity sent by server to [Consumer] when it is running.
type Delivery struct {
	delivery *amqp.Delivery
}

// Response is a code to be returned from [Handler]. It defines what kind of acknowledgement will consumer send on
// manual acknowledgement.
type Response uint8

func consumerDefaultConfig() consumerConfig {
	return consumerConfig{
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

// Start establishes channel to server and starts consuming [Delivery] and handle them using given [Handler]. Depending
// on [ConsumerOption], it also handles various notifications and errors from server.
func (c *Consumer) Start(handler Handler) error {
	var err error

	c.logger.Debug("Setting up consumer.")

	if err = c.channel.Connect(); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	c.logger.Debug("Declaring QoS.")

	if err = c.channel.Ch.Qos(c.config.prefetchCount, 0, false); err != nil {
		return fmt.Errorf("setting QoS: %w", err)
	}

	c.logger.Debug("Declaring consume.")

	c.deliveryChannel, err = c.channel.Ch.Consume(
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

	c.closeChannel = c.channel.Ch.NotifyClose(make(chan *amqp.Error))

	c.logger.Debug("Start consuming.")

	go c.consume(handler)

	c.logger.Debug("Consumer set up.")

	return nil
}

// Stop stops consuming [Delivery] and closes channel to server.
func (c *Consumer) Stop() error {
	c.logger.Debug("Stopping publisher.")

	if err := c.channel.Disconnect(); err != nil {
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
			// TODO: Do not honor result if consuming with auto acknowledge.
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

// Body returns body of a message itself.
func (d *Delivery) Body() []byte {
	return d.delivery.Body
}
