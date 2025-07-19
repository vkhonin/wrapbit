package primitive

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueBinding struct {
	Config QueueBindingConfig
}

type QueueBindingConfig struct {
	Queue    *Queue
	Key      string
	Exchange *Exchange
	NoWait   bool
	Args     amqp.Table
}

func QueueBindingDefaultConfig() QueueBindingConfig {
	return QueueBindingConfig{
		Queue:    nil,
		Key:      "",
		Exchange: nil,
		NoWait:   false,
		Args:     nil,
	}
}

func (b *QueueBinding) Declare(ch *amqp.Channel) error {
	if b.Config.Queue == nil {
		return fmt.Errorf("no queue to bind")
	}

	if err := b.Config.Queue.Declare(ch); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	if b.Config.Exchange == nil {
		return nil
	}

	if err := b.Config.Exchange.Declare(ch); err != nil {
		return fmt.Errorf("declare exchange: %w", err)
	}

	return ch.QueueBind(
		b.Config.Queue.Config.Name,
		b.Config.Key,
		b.Config.Exchange.Config.Name,
		b.Config.NoWait,
		b.Config.Args,
	)
}
