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
	p.wrapbit.logger.Debug("Setting up publisher.")

	ch, err := p.wrapbit.newChannel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	p.channel = ch

	p.wrapbit.logger.Debug("Publisher set up.")

	return nil
}

func (p *Publisher) Stop() error {
	p.wrapbit.logger.Debug("Stopping publisher.")

	if p.channel == nil {
		p.wrapbit.logger.Debug("Publisher stopped (no channel).")

		return nil
	}

	p.wrapbit.logger.Debug("Closing channel.")

	if err := p.channel.Close(); err != nil {
		return err
	}

	p.wrapbit.logger.Debug("Publisher stopped.")

	return nil
}

func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	p.wrapbit.logger.Debug("Preparing publishing.")

	for _, option := range options {
		if err := option(p); err != nil {
			return fmt.Errorf("apply Publisher options on Publish: %w", err)
		}
	}

	p.wrapbit.logger.Debug("Checking block before publish.")

	p.wrapbit.waitBlocked()

	p.wrapbit.logger.Debug("Publishing.")

	if err := p.channel.Publish(
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

	p.wrapbit.logger.Debug("Published.")

	return nil
}
