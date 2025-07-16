package wrapbit

import (
	"fmt"
	"github.com/vkhonin/wrapbit/internal/primitive"
	"slices"
)

// Option is a function that applies when [Wrapbit] instance is created and changes its settings.
type Option func(w *Wrapbit) error

// ExchangeOption is a function that applies when exchange is created and changes its settings.
type ExchangeOption func(q *primitive.Exchange) error

// WithExchange will declare exchange with given name and options on [Wrapbit.Start]. If no [ExchangeOption] supplied,
// durable direct exchange with given name will be declared.
func WithExchange(name string, options ...ExchangeOption) Option {
	return func(w *Wrapbit) error {
		e := new(primitive.Exchange)

		e.Config = primitive.ExchangeDefaultConfig()
		e.Config.Name = name

		for _, option := range options {
			if err := option(e); err != nil {
				return fmt.Errorf("apply Queue options: %w", err)
			}
		}

		w.exchanges[name] = e

		return nil
	}
}

// WithNode will use given URI to connect to server on [Wrapbit.Start]. If option is used multiple times, all unique
// URIs will be stored in given order. On [Wrapbit.Start], first successfully connected node will be used.
func WithNode(newURI string) Option {
	return func(w *Wrapbit) error {
		if !slices.Contains(w.config.clusterURIs, newURI) {
			w.config.clusterURIs = append(w.config.clusterURIs, newURI)
		}

		return nil
	}
}

// WithQueue will declare queue with given name and options on [Wrapbit.Start]. If no [QueueOption] supplied,
// durable queue with given name will be declared.
func WithQueue(name string, options ...QueueOption) Option {
	return func(w *Wrapbit) error {
		q := new(primitive.Queue)

		q.Config = primitive.QueueDefaultConfig()
		q.Config.Name = name

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
	}
}

// WithQueueBinding binds queue to exchange by their names using [QueueBindingOption] on [Wrapbit.Start]. Queue and
// exchange should be declared with corresponding [WithQueue] and [WithExchange] options.
func WithQueueBinding(queue, exchange string, options ...QueueBindingOption) Option {
	return func(w *Wrapbit) error {
		b := new(primitive.QueueBinding)

		b.Config = primitive.QueueBindingDefaultConfig()
		b.Config.Name = queue
		b.Config.Exchange = exchange

		for _, option := range options {
			if err := option(b); err != nil {
				return fmt.Errorf("apply QueueBinding options: %w", err)
			}
		}

		// TODO: This kind of storage should be replaced with something more sensible
		w.queueBindings[fmt.Sprintf("%s:%s:%s", b.Config.Exchange, b.Config.Key, b.Config.Name)] = b

		return nil
	}
}

// WithSingleConnection makes [Wrapbit] open only one connection on [Wrapbit.Start]. Normally [Wrapbit] uses two
// separate connections - one for publisher and one for consumers and system activities. Separate connection for
// publishers is required for TCP pushback to not affect consumers and system activities.
func WithSingleConnection() Option {
	return func(w *Wrapbit) error {
		delete(w.connections, publishConn)

		return nil
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
type QueueOption func(q *primitive.Queue) error

// QueueBindingOption is a function that applies when queue is being bound to exchange and changes binding settings.
type QueueBindingOption func(q *primitive.QueueBinding) error

// WithQueueBindingRoutingKey sets routing key for queue to exchange binding.
func WithQueueBindingRoutingKey(key string) QueueBindingOption {
	return func(b *primitive.QueueBinding) error {
		b.Config.Key = key

		return nil
	}
}
