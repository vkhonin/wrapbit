package wrapbit

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
	channel *channel
	config  PublisherConfig
	logger  Logger
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
	p.logger.Debug("Setting up publisher.")

	if err := p.channel.connect(); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	p.logger.Debug("Publisher set up.")

	return nil
}

func (p *Publisher) Stop() error {
	p.logger.Debug("Stopping publisher.")

	if err := p.channel.disconnect(); err != nil {
		return err
	}

	p.logger.Debug("Publisher stopped.")

	return nil
}

func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	p.logger.Debug("Preparing publishing.")

	for _, option := range options {
		if err := option(p); err != nil {
			return fmt.Errorf("apply Publisher options on Publish: %w", err)
		}
	}

	p.logger.Debug("Checking block before publish.")

	p.channel.waitBlocked()

	p.logger.Debug("Publishing.")

	if err := p.channel.ch.Publish(
		p.config.exchange,
		p.config.routingKey,
		p.config.mandatory,
		p.config.immediate,
		amqp.Publishing{
			Body: data,
		},
	); err != nil {
		return err
	}

	p.logger.Debug("Published.")

	return nil
}
