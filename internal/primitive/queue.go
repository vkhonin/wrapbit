package primitive

import amqp "github.com/rabbitmq/amqp091-go"

type Queue struct {
	Config QueueConfig

	Queue amqp.Queue
}

type QueueConfig struct {
	Name       string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table
}

type QueueOption func(q *Queue) error

func QueueDefaultConfig() QueueConfig {
	return QueueConfig{
		Name:       "",
		Durable:    true,
		AutoDelete: false,
		Exclusive:  false,
		NoWait:     false,
		Args:       nil,
	}
}
