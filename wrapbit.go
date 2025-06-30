package wrapbit

import (
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"slices"
)

type Wrapbit struct {
	channel    *amqp091.Channel
	config     WrapbitConfig
	connection *amqp091.Connection
	publishers map[string]*Publisher
	queues     map[string]*Queue
}

type WrapbitConfig struct {
	clusterURIs []string
}

type WrapbitOption func(w *Wrapbit) error

func NewInstance(options ...WrapbitOption) (*Wrapbit, error) {
	w := new(Wrapbit)
	w.config = wrapbitDefaultConfig()
	w.publishers = make(map[string]*Publisher)
	w.queues = make(map[string]*Queue)

	for _, option := range options {
		if err := option(w); err != nil {
			return nil, fmt.Errorf("apply Wrapbit options: %w", err)
		}
	}

	return w, nil
}

func wrapbitDefaultConfig() WrapbitConfig {
	return WrapbitConfig{
		clusterURIs: nil,
	}
}

// WithNode appends given AMQP URI to list of those used to establish connection. If there are multiple URIs in list,
// they will be handled as cluster.
func WithNode(newURI string) WrapbitOption {
	return func(w *Wrapbit) error {
		if !slices.Contains(w.config.clusterURIs, newURI) {
			w.config.clusterURIs = append(w.config.clusterURIs, newURI)
		}

		return nil
	}
}

// WithQueue declares given queue
func WithQueue(name string, options ...QueueOption) WrapbitOption {
	return func(w *Wrapbit) error {
		q := new(Queue)

		q.config = queueDefaultConfig()
		q.config.name = name

		for _, option := range options {
			if err := option(q); err != nil {
				return fmt.Errorf("apply Queue options: %w", err)
			}
		}

		w.queues[name] = q

		return nil
	}
}

func (w *Wrapbit) Start() error {
	connErrs := make([]error, len(w.config.clusterURIs))
	for _, uri := range w.config.clusterURIs {
		conn, err := amqp091.Dial(uri)
		if err != nil {
			connErrs = append(connErrs, err)

			continue
		}

		w.connection = conn

		break
	}

	if w.connection == nil {
		var err error
		if len(w.config.clusterURIs) == 0 {
			err = errors.New("no uris to connect")
		} else {
			err = errors.Join(connErrs...)
		}

		return fmt.Errorf("establish connection: %w", err)
	}

	ch, err := w.connection.Channel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	for _, q := range w.queues {
		_, err = w.channel.QueueDeclare(
			q.config.name,
			q.config.durable,
			q.config.autoDelete,
			q.config.exclusive,
			q.config.noWait,
			q.config.args,
		)
		if err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	return nil
}

func (w *Wrapbit) Stop() error {
	if err := w.connection.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}

	return nil
}

func (w *Wrapbit) NewPublisher(name string, options ...PublisherOption) (*Publisher, error) {
	if _, exists := w.publishers[name]; exists {
		return nil, fmt.Errorf("publisher with name %q exists", name)
	}

	p := new(Publisher)

	p.wrapbit = w

	for _, option := range options {
		if err := option(p); err != nil {
			return nil, fmt.Errorf("apply Publisher options: %w", err)
		}
	}

	w.publishers[name] = p

	return p, nil
}

func (w *Wrapbit) NewConsumer(queue string, options ...ConsumerOption) (*Consumer, error) {
	c := new(Consumer)

	c.wrapbit = w
	c.config.queue = queue

	for _, option := range options {
		if err := option(c); err != nil {
			return nil, fmt.Errorf("apply Consumer options: %w", err)
		}
	}

	return c, nil
}
