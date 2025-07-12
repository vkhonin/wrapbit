package wrapbit

import amqp "github.com/rabbitmq/amqp091-go"

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
	args       amqp.Table
}

type ExchangeOption func(q *Exchange) error

func exchangeDefaultConfig() ExchangeConfig {
	return ExchangeConfig{
		name:       "",
		kind:       amqp.ExchangeDirect,
		durable:    true,
		autoDelete: false,
		internal:   false,
		noWait:     false,
		args:       nil,
	}
}
