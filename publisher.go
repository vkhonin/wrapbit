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
		exchange:   amqp091.DefaultExchange,
		routingKey: "",
		mandatory:  false,
		immediate:  false,
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
	return p.channel.Close()
}

func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	p.wrapbit.waitBlocked()

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
