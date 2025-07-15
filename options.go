package wrapbit

import (
	"fmt"
	"github.com/vkhonin/wrapbit/internal/primitive"
	"slices"
)

// WithExchange declares given exchange
func WithExchange(name string, options ...primitive.ExchangeOption) Option {
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

// WithNode appends given AMQP URI to list of those used to establish connection. If there are multiple URIs in list,
// they will be handled as cluster.
func WithNode(newURI string) Option {
	return func(w *Wrapbit) error {
		if !slices.Contains(w.config.clusterURIs, newURI) {
			w.config.clusterURIs = append(w.config.clusterURIs, newURI)
		}

		return nil
	}
}

// WithQueue declares given queue
func WithQueue(name string, options ...primitive.QueueOption) Option {
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

// WithQueueBinding binds given queue to given exchange
func WithQueueBinding(queue, exchange string, options ...primitive.QueueBindingOption) Option {
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

// WithSeparateConnections makes Wrapbit use two separate connections - one for publishing, one for consuming.
func WithSeparateConnections() Option {
	return func(w *Wrapbit) error {
		w.connections[publishConn] = w.newConnection()

		return nil
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

// WithAutoReconnect enables automatic reconnection on consumer channel error
func WithAutoReconnect() ConsumerOption {
	return func(c *Consumer) error {
		c.config.autoReconnect = true

		return nil
	}
}

// WithPrefetchCount sets maximum number of deliveries sent by server without acknowledgement
func WithPrefetchCount(n int) ConsumerOption {
	return func(c *Consumer) error {
		c.config.prefetchCount = n

		return nil
	}
}

func WithQueueBindingRoutingKey(key string) primitive.QueueBindingOption {
	return func(b *primitive.QueueBinding) error {
		b.Config.Key = key

		return nil
	}
}
