package wrapbit

import "github.com/rabbitmq/amqp091-go"

type Exchange struct {
	config ExchangeConfig
}

type ExchangeConfig struct {
	name       string
	kind       string
	durable    bool
	autoDelete bool
	internal   bool
	noWait     bool
	args       amqp091.Table
}

type ExchangeOption func(q *Exchange) error

func exchangeDefaultConfig() ExchangeConfig {
	return ExchangeConfig{
		name:       "",
		kind:       amqp091.ExchangeDirect,
		durable:    true,
		autoDelete: false,
		internal:   false,
		noWait:     false,
		args:       nil,
	}
}
