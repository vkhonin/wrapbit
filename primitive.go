package wrapbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type exchange struct {
	config exchangeConfig
}

type exchangeConfig struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp.Table
}

func exchangeDefaultConfig() exchangeConfig {
	return exchangeConfig{
		name:       "",
		kind:       amqp.ExchangeDirect,
		durable:    true,
		autoDelete: false,
		internal:   false,
		noWait:     false,
		args:       nil,
	}
}

func (q *exchange) declare(ch *amqp.Channel) error {
	return ch.ExchangeDeclare(
		q.config.name,
		q.config.kind,
		q.config.durable,
		q.config.autoDelete,
		q.config.internal,
		q.config.noWait,
		q.config.args,
	)
}

type queue struct {
	config queueConfig
	queue  amqp.Queue
}

type queueConfig struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp.Table
}

func queueDefaultConfig() queueConfig {
	return queueConfig{
		name:       "",
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}
}

func (q *queue) declare(ch *amqp.Channel) (err error) {
	q.queue, err = ch.QueueDeclare(
		q.config.name,
		q.config.durable,
		q.config.autoDelete,
		q.config.exclusive,
		q.config.noWait,
		q.config.args,
	)

	return
}

type queueBinding struct {
	config queueBindingConfig
}

type queueBindingConfig struct {
	queue    *queue
	key      string
	exchange *exchange
	noWait   bool
	args     amqp.Table
}

func queueBindingDefaultConfig() queueBindingConfig {
	return queueBindingConfig{
		queue:    nil,
		key:      "",
		exchange: nil,
		noWait:   false,
		args:     nil,
	}
}

func (b *queueBinding) declare(ch *amqp.Channel) error {
	if b.config.queue == nil {
		return fmt.Errorf("no queue to bind")
	}

	if err := b.config.queue.declare(ch); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if b.config.exchange == nil {
		return nil
	}

	if err := b.config.exchange.declare(ch); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	return ch.QueueBind(
		b.config.queue.config.name,
		b.config.key,
		b.config.exchange.config.name,
		b.config.noWait,
		b.config.args,
	)
}
