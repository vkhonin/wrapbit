package primitive

import amqp "github.com/rabbitmq/amqp091-go"

type Exchange struct {
	Config ExchangeConfig
}

type ExchangeConfig struct {
	Name       string
	Kind       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table
}

func ExchangeDefaultConfig() ExchangeConfig {
	return ExchangeConfig{
		Name:       "",
		Kind:       amqp.ExchangeDirect,
		Durable:    true,
		AutoDelete: false,
		Internal:   false,
		NoWait:     false,
		Args:       nil,
	}
}
