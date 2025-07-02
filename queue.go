package wrapbit

import "github.com/rabbitmq/amqp091-go"

type Queue struct {
	config QueueConfig
	queue  amqp091.Queue
}

type QueueConfig struct {
	name       string
	durable    bool
	autoDelete bool
	exclusive  bool
	noWait     bool
	args       amqp091.Table
}

type QueueOption func(q *Queue) error

func queueDefaultConfig() QueueConfig {
	return QueueConfig{
		name:       "",
		durable:    true,
		autoDelete: false,
		exclusive:  false,
		noWait:     false,
		args:       nil,
	}
}

type QueueBinding struct {
	config QueueBindingConfig
}

type QueueBindingConfig struct {
	name     string
	key      string
	exchange string
	noWait   bool
	args     amqp091.Table
}

type QueueBindingOption func(q *QueueBinding) error

func queueBindingDefaultConfig() QueueBindingConfig {
	return QueueBindingConfig{
		name:     "",
		key:      "",
		exchange: "",
		noWait:   false,
		args:     nil,
	}
}

func WithQueueBindingRoutingKey(key string) QueueBindingOption {
	return func(b *QueueBinding) error {
		b.config.key = key

		return nil
	}
}
