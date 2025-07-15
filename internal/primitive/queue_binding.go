package primitive

import amqp "github.com/rabbitmq/amqp091-go"

type QueueBinding struct {
	Config QueueBindingConfig
}

type QueueBindingConfig struct {
	Name     string
	Key      string
	Exchange string
	NoWait   bool
	Args     amqp.Table
}

type QueueBindingOption func(q *QueueBinding) error

func QueueBindingDefaultConfig() QueueBindingConfig {
	return QueueBindingConfig{
		Name:     "",
		Key:      "",
		Exchange: "",
		NoWait:   false,
		Args:     nil,
	}
}

func WithQueueBindingRoutingKey(key string) QueueBindingOption {
	return func(b *QueueBinding) error {
		b.Config.Key = key

		return nil
	}
}
