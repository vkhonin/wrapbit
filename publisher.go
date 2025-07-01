package wrapbit

import (
	"fmt"
	"github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	channel *amqp091.Channel
	config  PublisherConfig
	wrapbit *Wrapbit
}

type PublisherConfig struct {
	exchange   string
	routingKey string
	mandatory  bool
	immediate  bool
}

type PublisherOption func(p *Publisher) error

func publisherDefaultConfig() PublisherConfig {
	return PublisherConfig{
		exchange:   "",
		routingKey: "",
		mandatory:  false,
		immediate:  false,
	}
}

// WithRoutingKey sets default routing key to be used on Publish
func WithRoutingKey(routingKey string) PublisherOption {
	return func(p *Publisher) error {
		p.config.routingKey = routingKey

		return nil
	}
}

func (p *Publisher) Start() error {
	ch, err := p.wrapbit.newChannel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	p.channel = ch

	return nil
}

func (p *Publisher) Stop() error {
	return p.channel.Close()
}

func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	return p.channel.Publish(
		p.config.exchange,
		p.config.routingKey,
		p.config.mandatory,
		p.config.immediate,
		amqp091.Publishing{
			Body: data,
		},
	)
}
