package wrapbit

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

// Wrapbit is an instance that manages server interaction and creates [Consumer] and [Producer].
type Wrapbit struct {
	channel       *channel
	config        config
	connections   []*connection
	exchanges     map[string]*exchange
	logger        Logger
	queueBindings []*queueBinding
	queues        map[string]*queue
}

type config struct {
	clusterURIs             []string
	channelRetryStrategy    Retry
	connectionRetryStrategy Retry
}

// New creates [Wrapbit] instance with given [Option].
func New(options ...Option) (*Wrapbit, error) {
	w := new(Wrapbit)

	w.initLogger()

	w.logger.Debug("Setting up Wrapbit instance.")

	w.initFields()

	w.logger.Debug("Applying Wrapbit options.")

	if err := w.applyOptions(options); err != nil {
		return nil, fmt.Errorf("apply Wrapbit options: %w", err)
	}

	w.logger.Debug("Creating connections.")

	for i := range 2 {
		w.connections[i] = w.newConnection()
	}

	w.logger.Debug("Wrapbit instance set up.")

	return w, nil
}

// Start establishes connections to server and declares queues, exchanges and bindings.
func (w *Wrapbit) Start() error {
	w.logger.Debug("Starting Wrapbit instance.")

	w.logger.Debug("Establishing connections.")

	for _, conn := range w.connections {
		if err := conn.connect(context.TODO()); err != nil {
			return err
		}
	}

	ch := w.connections[0].newChannel()

	if err := ch.connect(context.TODO()); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	w.logger.Debug("Declaring queues.")

	var err error

	for _, q := range w.queues {
		if err = q.declare(w.channel.ch); err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	w.logger.Debug("Declaring exchanges.")

	for _, e := range w.exchanges {
		if err = e.declare(w.channel.ch); err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}

	w.logger.Debug("Binding queues.")

	for _, b := range w.queueBindings {
		if err = b.declare(w.channel.ch); err != nil {
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
		if err := conn.disconnect(); err != nil {
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

	p.channel = w.connections[1].newChannel()
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

	c.channel = w.connections[0].newChannel()
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

func (w *Wrapbit) applyOptions(opts []Option) error {
	slices.SortFunc(opts, func(a, b Option) int {
		return a.w - b.w
	})

	for _, opt := range opts {
		if err := opt.f(w); err != nil {
			return err
		}
	}

	return nil
}

func (w *Wrapbit) initFields() {
	w.config = config{
		clusterURIs: nil,
		channelRetryStrategy: func() Attempter {
			return &linearAttempter{
				backoff:     50 * time.Millisecond,
				maxAttempts: 10,
			}
		},
		connectionRetryStrategy: func() Attempter {
			return &exponentialAttempter{
				baseBackoff: 500 * time.Millisecond,
			}
		},
	}
	w.connections = make([]*connection, 2)
	w.exchanges = make(map[string]*exchange)
	w.queues = make(map[string]*queue)
}

func (w *Wrapbit) initLogger() {
	var (
		env   string
		found bool
	)

	env, found = os.LookupEnv("WRAPBIT_DEBUG_LOG_LEVEL")

	if !found {
		w.logger = new(nullLogger)

		return
	}

	l := new(debugLogger)
	lvl, _ := strconv.Atoi(env)
	l.setLevel(level(lvl))

	w.logger = l
}

func (w *Wrapbit) newConnection() *connection {
	c := connection{
		blockCond: sync.NewCond(&sync.Mutex{}),
		chRetry:   w.config.channelRetryStrategy,
		logger:    w.logger,
		retry:     w.config.connectionRetryStrategy,
		uris:      w.config.clusterURIs,
	}

	return &c
}

// restoreQueue restores queue and its bindings by name.
//
// TODO: should be refactored.
func (w *Wrapbit) restoreQueue(name string) error {
	q, ok := w.queues[name]
	if !ok {
		return fmt.Errorf("no %q queue", name)
	}

	if err := q.declare(w.channel.ch); err != nil {
		return fmt.Errorf("declare queue: %w", err)
	}

	for _, b := range w.queueBindings {
		if err := b.declare(w.channel.ch); err != nil {
			return fmt.Errorf("binding queue: %w", err)
		}
	}

	return nil
}
