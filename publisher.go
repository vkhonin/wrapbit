package wrapbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	channel *amqp.Channel
	config  PublisherConfig
	wrapbit *Wrapbit
}

type PublisherConfig struct {
	exchange   string
	immediate  bool
	mandatory  bool
	routingKey string
}

type PublisherOption func(p *Publisher) error

func publisherDefaultConfig() PublisherConfig {
	return PublisherConfig{
		exchange:   amqp.DefaultExchange,
		immediate:  false,
		mandatory:  false,
		routingKey: "",
	}
}

// WithPublisherExchange sets default exchange to be used on Publish
func WithPublisherExchange(exchange string) PublisherOption {
	return func(p *Publisher) error {
		p.config.exchange = exchange

		return nil
	}
}

// WithPublisherRoutingKey sets default routing key to be used on Publish
func WithPublisherRoutingKey(routingKey string) PublisherOption {
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
	if p.channel == nil {
		return nil
	}

	return p.channel.Close()
}

func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	for _, option := range options {
		if err := option(p); err != nil {
			return fmt.Errorf("apply Publisher options on Publish: %w", err)
		}
	}

	p.wrapbit.waitBlocked()

	return p.channel.Publish(
		p.config.exchange,
		p.config.routingKey,
		p.config.mandatory,
		p.config.immediate,
		amqp.Publishing{
			Body: data,
		},
	)
}
