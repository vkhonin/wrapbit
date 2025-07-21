package wrapbit

import (
	"fmt"
	"slices"
)

const (
	neutralW = iota
	queueBindingW
)

// Option is a function that applies when [Wrapbit] instance is created and changes its settings.
type Option struct {
	f func(w *Wrapbit) error
	w int
}

// ExchangeOption is a function that applies when exchange is created and changes its settings.
type ExchangeOption func(q *exchange) error

// WithExchange will declare exchange with given name and options on [Wrapbit.Start]. If no [ExchangeOption] supplied,
// durable direct exchange with given name will be declared.
func WithExchange(name string, options ...ExchangeOption) Option {
	return Option{
		f: func(w *Wrapbit) error {
			e := new(exchange)

			e.config = exchangeDefaultConfig()
			e.config.name = name

			for _, option := range options {
				if err := option(e); err != nil {
					return fmt.Errorf("apply Queue options: %w", err)
				}
			}

			w.exchanges[name] = e

			return nil
		},
		w: neutralW,
	}
}

// WithNode will use given URI to connect to server on [Wrapbit.Start]. If option is used multiple times, all unique
// URIs will be stored in given order. On [Wrapbit.Start], first successfully connected node will be used.
func WithNode(newURI string) Option {
	return Option{
		f: func(w *Wrapbit) error {
			if !slices.Contains(w.config.clusterURIs, newURI) {
				w.config.clusterURIs = append(w.config.clusterURIs, newURI)
			}

			return nil
		},
		w: neutralW,
	}
}

// WithQueue will declare queue with given name and options on [Wrapbit.Start]. If no [QueueOption] supplied,
// durable queue with given name will be declared.
func WithQueue(name string, options ...QueueOption) Option {
	return Option{
		f: func(w *Wrapbit) error {
			q := new(queue)

			q.config = queueDefaultConfig()
			q.config.name = name

			for _, option := range options {
				if err := option(q); err != nil {
					return fmt.Errorf("apply Queue options: %w", err)
				}
			}

			// TODO: As queues can be declared without name (it will be assigned by server on declaration), there will be
			// conflict below. We need some logic to:
			// 1. Store nameless queues before actual declaration.
			// 2. Update their mappings after declaration, when the actual name will be assigned by the server.
			w.queues[name] = q

			return nil
		},
		w: neutralW,
	}
}

// WithQueueBinding binds queue to exchange by their names using [QueueBindingOption] on [Wrapbit.Start]. Queue and
// exchange should be declared with corresponding [WithQueue] and [WithExchange] options.
func WithQueueBinding(queue, exchange string, options ...QueueBindingOption) Option {
	return Option{
		f: func(w *Wrapbit) error {
			b := new(queueBinding)

			b.config = queueBindingDefaultConfig()

			var ok bool

			b.config.queue, ok = w.queues[queue]
			if !ok {
				return fmt.Errorf("no %q queue", queue)
			}

			b.config.exchange, ok = w.exchanges[exchange]
			if !ok {
				return fmt.Errorf("no %q exchange", exchange)
			}

			for _, option := range options {
				if err := option(b); err != nil {
					return fmt.Errorf("apply QueueBinding options: %w", err)
				}
			}

			w.queueBindings = append(w.queueBindings, b)

			return nil
		},
		w: queueBindingW,
	}
}

// PublisherOption is a function that applies when [Publisher] is created and changes its settings.
type PublisherOption func(p *Publisher) error

// WithPublisherExchange sets exchange to be used on [Publisher.Publish]. Exchange should be declared using
// [WithExchange] options.
func WithPublisherExchange(exchange string) PublisherOption {
	return func(p *Publisher) error {
		p.config.exchange = exchange

		return nil
	}
}

// WithPublisherRoutingKey sets routing key to be used on [Publisher.Publish].
func WithPublisherRoutingKey(routingKey string) PublisherOption {
	return func(p *Publisher) error {
		p.config.routingKey = routingKey

		return nil
	}
}

// ConsumerOption is a function that applies when [Consumer] is created and changes its settings.
type ConsumerOption func(p *Consumer) error

// WithAutoReconnect makes [Consumer] open new channel when current channel is closed for any reason.
func WithAutoReconnect() ConsumerOption {
	return func(c *Consumer) error {
		c.config.autoReconnect = true

		return nil
	}
}

// WithPrefetchCount makes server send this many [Delivery] without acknowledgement from [Consumer].
func WithPrefetchCount(n int) ConsumerOption {
	return func(c *Consumer) error {
		c.config.prefetchCount = n

		return nil
	}
}

// QueueOption is a function that applies when queue is created and changes its settings.
type QueueOption func(q *queue) error

// QueueBindingOption is a function that applies when queue is being bound to exchange and changes binding settings.
type QueueBindingOption func(q *queueBinding) error

// WithQueueBindingRoutingKey sets routing key for queue to exchange binding.
func WithQueueBindingRoutingKey(key string) QueueBindingOption {
	return func(b *queueBinding) error {
		b.config.key = key

		return nil
	}
}
