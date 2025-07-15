package wrapbit

import (
	"fmt"
	"github.com/vkhonin/wrapbit/internal/attempter"
	"github.com/vkhonin/wrapbit/internal/logger"
	"github.com/vkhonin/wrapbit/internal/primitive"
	"github.com/vkhonin/wrapbit/internal/transport"
	"github.com/vkhonin/wrapbit/utils"
	"os"
	"slices"
	"strconv"
	"time"
)

const (
	commonConn  = "common"
	publishConn = "publish"
)

type Wrapbit struct {
	channel       *transport.Channel
	config        Config
	connections   map[string]*transport.Connection
	exchanges     map[string]*primitive.Exchange
	logger        utils.Logger
	publishers    map[string]*Publisher
	queueBindings map[string]*primitive.QueueBinding
	queues        map[string]*primitive.Queue
}

type Config struct {
	clusterURIs             []string
	channelRetryStrategy    utils.Retry
	connectionRetryStrategy utils.Retry
}

type Option func(w *Wrapbit) error

func New(options ...Option) (*Wrapbit, error) {
	w := new(Wrapbit)

	w.logger = new(logger.NullLogger)
	if env, found := os.LookupEnv("WRAPBIT_DEBUG_LOG_LEVEL"); found {
		l := new(logger.DebugLogger)
		level, _ := strconv.Atoi(env)
		l.SetLevel(logger.Level(level))
		w.logger = l
	}

	w.logger.Debug("Setting up Wrapbit instance.")

	w.config = defaultConfig()
	w.connections = make(map[string]*transport.Connection)
	w.exchanges = make(map[string]*primitive.Exchange)
	w.publishers = make(map[string]*Publisher)
	w.queueBindings = make(map[string]*primitive.QueueBinding)
	w.queues = make(map[string]*primitive.Queue)

	w.logger.Debug("Applying Wrapbit options.")

	for _, option := range options {
		if err := option(w); err != nil {
			return nil, fmt.Errorf("apply Wrapbit options: %w", err)
		}
	}

	w.connections[commonConn] = w.newConnection()

	w.logger.Debug("Wrapbit options applied.")
	w.logger.Debug("Wrapbit instance set up.")

	return w, nil
}

func defaultConfig() Config {
	return Config{
		clusterURIs:             nil,
		channelRetryStrategy:    defaultChannelRetryStrategy,
		connectionRetryStrategy: defaultConnectionRetryStrategy,
	}
}

func defaultConnectionRetryStrategy() utils.Attempter {
	return &attempter.ExponentialAttempter{
		BaseBackoff: 500 * time.Millisecond,
	}
}

func defaultChannelRetryStrategy() utils.Attempter {
	return &attempter.LinearAttempter{
		Backoff:     50 * time.Millisecond,
		MaxAttempts: 10,
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

// WithSeparateConnections makes Wrapbit use two separate connections - one for publishing, one for consuming.
func WithSeparateConnections() Option {
	return func(w *Wrapbit) error {
		w.connections[publishConn] = w.newConnection()

		return nil
	}
}

func (w *Wrapbit) Start() error {
	w.logger.Debug("Starting Wrapbit instance.")

	for _, conn := range w.connections {
		if err := conn.Connect(); err != nil {
			return err
		}
	}

	ch := w.connections[commonConn].NewChannel()

	if err := ch.Connect(); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	w.logger.Debug("Declaring queues.")

	var err error

	for _, q := range w.queues {
		c := &q.Config
		q.Queue, err = w.channel.Ch.QueueDeclare(c.Name, c.Durable, c.AutoDelete, c.Exclusive, c.NoWait, c.Args)
		if err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	w.logger.Debug("Declaring exchanges.")

	for _, e := range w.exchanges {
		c := &e.Config
		err = w.channel.Ch.ExchangeDeclare(c.Name, c.Kind, c.Durable, c.AutoDelete, c.Internal, c.NoWait, c.Args)
		if err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}

	w.logger.Debug("Binding queues.")

	for _, b := range w.queueBindings {
		c := &b.Config
		err = w.channel.Ch.QueueBind(c.Name, c.Key, c.Exchange, c.NoWait, c.Args)
		if err != nil {
			return fmt.Errorf("binding queue: %w", err)
		}
	}

	w.logger.Debug("Wrapbit instance started.")

	return nil
}

func (w *Wrapbit) Stop() error {
	w.logger.Debug("Stopping Wrapbit instance.")

	for _, conn := range w.connections {
		if err := conn.Disconnect(); err != nil {
			return fmt.Errorf("wrapbit stop: %w", err)
		}
	}

	// TODO: Add publishers connection as well.

	w.logger.Debug("Wrapbit instance stopped.")

	return nil
}

func (w *Wrapbit) NewPublisher(name string, options ...PublisherOption) (*Publisher, error) {
	if _, exists := w.publishers[name]; exists {
		return nil, fmt.Errorf("publisher with name %q exists", name)
	}

	w.logger.Debug("Setting up Publisher instance.")

	p := new(Publisher)

	p.channel = w.connections[publishConn].NewChannel()
	p.config = publisherDefaultConfig()
	p.logger = w.logger

	w.logger.Debug("Applying Publisher options.")

	for _, option := range options {
		if err := option(p); err != nil {
			return nil, fmt.Errorf("apply Publisher options: %w", err)
		}
	}

	w.publishers[name] = p

	w.logger.Debug("Publisher instance set up.")

	return p, nil
}

func (w *Wrapbit) NewConsumer(queue string, options ...ConsumerOption) (*Consumer, error) {
	w.logger.Debug("Setting up Consumer instance.")

	c := new(Consumer)

	c.channel = w.connections[commonConn].NewChannel()
	c.config = consumerDefaultConfig()
	c.config.queue = queue
	c.logger = w.logger

	w.logger.Debug("Applying Consumer options.")

	for _, option := range options {
		if err := option(c); err != nil {
			return nil, fmt.Errorf("apply Consumer options: %w", err)
		}
	}

	w.logger.Debug("Consumer instance set up.")

	return c, nil
}

func (w *Wrapbit) newConnection() *transport.Connection {
	c := transport.Connection{
		BlockChan: make(chan struct{}),
		ChRetry:   w.config.channelRetryStrategy,
		Logger:    w.logger,
		Retry:     w.config.connectionRetryStrategy,
		URIs:      w.config.clusterURIs,
	}

	close(c.BlockChan)

	return &c
}
