package wrapbit

import (
	"context"
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

// Wrapbit is an instance that manages server interaction and creates [Consumer] and [Producer].
type Wrapbit struct {
	channel       *transport.Channel
	config        config
	connections   []*transport.Connection
	exchanges     map[string]*primitive.Exchange
	logger        utils.Logger
	queueBindings []*primitive.QueueBinding
	queues        map[string]*primitive.Queue
}

type config struct {
	clusterURIs             []string
	channelRetryStrategy    utils.Retry
	connectionRetryStrategy utils.Retry
}

// New creates [Wrapbit] instance with given [Option].
func New(options ...Option) (*Wrapbit, error) {
	w := new(Wrapbit)

	w.logger = defaultLogger()

	w.logger.Debug("Setting up Wrapbit instance.")

	w.config = defaultConfig()

	w.connections = make([]*transport.Connection, 2)
	w.exchanges = make(map[string]*primitive.Exchange)
	w.queues = make(map[string]*primitive.Queue)

	slices.SortFunc(options, func(a, b Option) int {
		return a.w - b.w
	})

	for _, option := range options {
		if err := option.f(w); err != nil {
			return nil, fmt.Errorf("apply Wrapbit option: %w", err)
		}
	}

	w.logger.Debug("Wrapbit options applied.")

	for i := range 2 {
		w.connections[i] = w.newConnection()
	}

	w.logger.Debug("Wrapbit connections created.")

	w.logger.Debug("Wrapbit instance set up.")

	return w, nil
}

// Start establishes connections to server and declares queues, exchanges and bindings.
func (w *Wrapbit) Start() error {
	w.logger.Debug("Starting Wrapbit instance.")

	for _, conn := range w.connections {
		if err := conn.Connect(); err != nil {
			return err
		}
	}

	ch := w.connections[0].NewChannel()

	if err := ch.Connect(context.TODO()); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	w.logger.Debug("Declaring queues.")

	var err error

	for _, q := range w.queues {
		if err = q.Declare(w.channel.Ch); err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	w.logger.Debug("Declaring exchanges.")

	for _, e := range w.exchanges {
		if err = e.Declare(w.channel.Ch); err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}

	w.logger.Debug("Binding queues.")

	for _, b := range w.queueBindings {
		if err = b.Declare(w.channel.Ch); err != nil {
			return fmt.Errorf("binding queue: %w", err)
		}
	}

	w.logger.Debug("Wrapbit instance started.")

	return nil
}

// Stop closes all connections to server.
func (w *Wrapbit) Stop() error {
	w.logger.Debug("Stopping Wrapbit instance.")

	// TODO: when NotifyPublish will be implemented, we should wait for all Confirmations before closing Connection.
	for _, conn := range w.connections {
		if err := conn.Disconnect(); err != nil {
			return fmt.Errorf("wrapbit stop: %w", err)
		}
	}

	w.logger.Debug("Wrapbit instance stopped.")

	return nil
}

// NewPublisher creates publisher with given name and [PublisherOption].
func (w *Wrapbit) NewPublisher(name string, options ...PublisherOption) (*Publisher, error) {
	w.logger.Debug("Setting up Publisher instance.")

	p := new(Publisher)

	p.channel = w.connections[1].NewChannel()
	p.config = publisherDefaultConfig()
	p.logger = w.logger

	w.logger.Debug("Applying Publisher options.")

	for _, option := range options {
		if err := option(p); err != nil {
			return nil, fmt.Errorf("apply Publisher options: %w", err)
		}
	}

	w.logger.Debug("Publisher instance set up.")

	return p, nil
}

// NewConsumer creates publisher with given target queue and [ConsumerOption].
func (w *Wrapbit) NewConsumer(queueName string, options ...ConsumerOption) (*Consumer, error) {
	w.logger.Debug("Setting up Consumer instance.")

	c := new(Consumer)

	c.channel = w.connections[0].NewChannel()
	c.config = consumerDefaultConfig()
	c.logger = w.logger
	c.wrapbit = w

	var ok bool
	if c.config.queue, ok = w.queues[queueName]; !ok {
		return nil, fmt.Errorf("no %q queue", queueName)
	}

	w.logger.Debug("Applying Consumer options.")

	for _, option := range options {
		if err := option(c); err != nil {
			return nil, fmt.Errorf("apply Consumer options: %w", err)
		}
	}

	w.logger.Debug("Consumer instance set up.")

	return c, nil
}

func (w *Wrapbit) restoreQueue(name string) error {
	q, ok := w.queues[name]
	if !ok {
		return fmt.Errorf("no %q queue", name)
	}

	if err := q.Declare(w.channel.Ch); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	for _, b := range w.queueBindings {
		if err := b.Declare(w.channel.Ch); err != nil {
			return fmt.Errorf("binding queue: %w", err)
		}
	}

	return nil
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

func defaultChannelRetryStrategy() utils.Attempter {
	return &attempter.LinearAttempter{
		Backoff:     50 * time.Millisecond,
		MaxAttempts: 10,
	}
}

func defaultConfig() config {
	return config{
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

func defaultLogger() utils.Logger {
	var (
		env   string
		found bool
	)

	env, found = os.LookupEnv("WRAPBIT_DEBUG_LOG_LEVEL")

	if !found {
		return new(logger.NullLogger)
	}

	l := new(logger.DebugLogger)
	level, _ := strconv.Atoi(env)
	l.SetLevel(logger.Level(level))

	return l
}
