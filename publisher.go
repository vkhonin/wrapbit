package wrapbit

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/internal/transport"
	"github.com/vkhonin/wrapbit/utils"
)

// Publisher is an instance that publishes to its designated exchange.
type Publisher struct {
	channel *transport.Channel
	config  publisherConfig
	logger  utils.Logger
}

type publisherConfig struct {
	exchange   string
	immediate  bool
	mandatory  bool
	routingKey string
}

func publisherDefaultConfig() publisherConfig {
	return publisherConfig{
		exchange:   amqp.DefaultExchange,
		immediate:  false,
		mandatory:  false,
		routingKey: "",
	}
}

// Start establishes channel to server.
func (p *Publisher) Start() error {
	p.logger.Debug("Setting up publisher.")

	if err := p.channel.Connect(context.TODO()); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	if err := p.channel.Ch.Confirm(false); err != nil {
		return fmt.Errorf("channel confirm: %w", err)
	}

	p.logger.Debug("Publisher set up.")

	return nil
}

// Stop closes channel to server.
func (p *Publisher) Stop() error {
	p.logger.Debug("Stopping publisher.")

	if err := p.channel.Disconnect(); err != nil {
		return err
	}

	p.logger.Debug("Publisher stopped.")

	return nil
}

// Publish publishes given data to server with either current [Publisher] options, or with given [PublisherOption]
// applied to [Publisher] before publication. Note that [PublisherOption] application to [Publisher] is permanent.
func (p *Publisher) Publish(data []byte, options ...PublisherOption) error {
	p.logger.Debug("Preparing publishing.")

	// TODO: reason whether permanent options application is OK. The other option is to restore config after publication
	for _, option := range options {
		if err := option(p); err != nil {
			return fmt.Errorf("apply Publisher options on Publish: %w", err)
		}
	}

	p.logger.Debug("Publishing.")

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

	p.logger.Debug("Published.")

	return nil
}
