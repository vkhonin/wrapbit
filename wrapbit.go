package wrapbit

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/internal/attempter"
	"github.com/vkhonin/wrapbit/internal/logger"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"
)

const (
	commonConn  = "common"
	publishConn = "publish"
)

type Wrapbit struct {
	channel       *channel
	config        Config
	connections   map[string]*connection
	exchanges     map[string]*Exchange
	logger        Logger
	publishers    map[string]*Publisher
	queueBindings map[string]*QueueBinding
	queues        map[string]*Queue
}

type Config struct {
	clusterURIs             []string
	channelRetryStrategy    RetryStrategy
	connectionRetryStrategy RetryStrategy
}

type Option func(w *Wrapbit) error

type RetryStrategy func() Attempter

type Attempter interface {
	Attempt() bool
}

type Logger interface {
	Debug(args ...any)
	Error(args ...any)
	Info(args ...any)
	Warn(args ...any)
}

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
	w.connections = make(map[string]*connection)
	w.exchanges = make(map[string]*Exchange)
	w.publishers = make(map[string]*Publisher)
	w.queueBindings = make(map[string]*QueueBinding)
	w.queues = make(map[string]*Queue)

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

func defaultConnectionRetryStrategy() Attempter {
	return &attempter.ExponentialAttempter{
		BaseBackoff: 500 * time.Millisecond,
	}
}

func defaultChannelRetryStrategy() Attempter {
	return &attempter.LinearAttempter{
		Backoff:     50 * time.Millisecond,
		MaxAttempts: 10,
	}
}

// WithQueueBinding binds given queue to given exchange
func WithQueueBinding(queue, exchange string, options ...QueueBindingOption) Option {
	return func(w *Wrapbit) error {
		b := new(QueueBinding)

		b.config = queueBindingDefaultConfig()
		b.config.name = queue
		b.config.exchange = exchange

		for _, option := range options {
			if err := option(b); err != nil {
				return fmt.Errorf("apply QueueBinding options: %w", err)
			}
		}

		// TODO: This kind of storage should be replaced with something more sensible
		w.queueBindings[fmt.Sprintf("%s:%s:%s", b.config.exchange, b.config.key, b.config.name)] = b

		return nil
	}
}

// WithExchange declares given exchange
func WithExchange(name string, options ...ExchangeOption) Option {
	return func(w *Wrapbit) error {
		e := new(Exchange)

		e.config = exchangeDefaultConfig()
		e.config.name = name

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
func WithQueue(name string, options ...QueueOption) Option {
	return func(w *Wrapbit) error {
		q := new(Queue)

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
		if err := conn.connect(); err != nil {
			return err
		}
	}

	ch := w.connections[commonConn].newChannel()

	if err := ch.connect(); err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	w.logger.Debug("Declaring queues.")

	for _, q := range w.queues {
		c := &q.config
		_, err := w.channel.channel.QueueDeclare(c.name, c.durable, c.autoDelete, c.exclusive, c.noWait, c.args)
		if err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	w.logger.Debug("Declaring exchanges.")

	for _, e := range w.exchanges {
		c := &e.config
		err := w.channel.channel.ExchangeDeclare(c.name, c.kind, c.durable, c.autoDelete, c.internal, c.noWait, c.args)
		if err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}

	w.logger.Debug("Binding queues.")

	for _, b := range w.queueBindings {
		c := &b.config
		err := w.channel.channel.QueueBind(c.name, c.key, c.exchange, c.noWait, c.args)
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
		if err := conn.disconnect(); err != nil {
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

	p.channel = w.connections[publishConn].newChannel()
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

	c.channel = w.connections[commonConn].newChannel()
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

func (w *Wrapbit) newConnection() *connection {
	c := connection{
		blockedMu:       new(sync.RWMutex),
		blockedCh:       make(chan struct{}),
		chRetryStrategy: w.config.channelRetryStrategy,
		clusterURIs:     w.config.clusterURIs,
		connMu:          new(sync.RWMutex),
		conn:            nil,
		logger:          w.logger,
		retryStrategy:   w.config.connectionRetryStrategy,
	}

	close(c.blockedCh)

	return &c
}

type connection struct {
	blockedMu       *sync.RWMutex
	blockedCh       chan struct{}
	chRetryStrategy RetryStrategy
	clusterURIs     []string
	connMu          *sync.RWMutex
	conn            *amqp.Connection
	logger          Logger
	retryStrategy   RetryStrategy
}

func (c *connection) channel() (*amqp.Channel, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn.Channel()
}

func (c *connection) connect() error {
	c.logger.Debug("Setting up connection.")

	c.connMu.Lock()
	defer c.connMu.Unlock()

	var connErrs []error

dial:
	for a := c.retryStrategy(); a.Attempt(); {
		for _, uri := range c.clusterURIs {
			c.logger.Debug("Dialing.", uri)

			conn, err := amqp.Dial(uri)
			if err != nil {
				c.logger.Warn("Dial error.", uri, err)

				connErrs = append(connErrs, err)

				continue
			}

			c.conn = conn

			break dial
		}
	}

	if c.conn == nil {
		var err error
		if len(c.clusterURIs) == 0 {
			err = errors.New("no URIs to connect")
		} else {
			err = errors.Join(connErrs...)
		}

		return fmt.Errorf("establish connection: %w", err)
	}

	c.logger.Debug("Setting up connection notifications.")

	var (
		blockCh <-chan amqp.Blocking = c.conn.NotifyBlocked(make(chan amqp.Blocking, 1))
		closeCh <-chan *amqp.Error   = c.conn.NotifyClose(make(chan *amqp.Error, 1))
	)

	go func() {
		// Closes blockedCh if not closed yet, so that waiting instances will not hang forever.
		c.blockedMu.Lock()
		select {
		case <-c.blockedCh:
		default:
			if c.blockedCh != nil {
				close(c.blockedCh)
			}
		}
		c.blockedMu.Unlock()

		for blocking := range blockCh {
			c.blockedMu.Lock()
			if blocking.Active {
				c.blockedCh = make(chan struct{})
			} else {
				close(c.blockedCh)
			}
			c.blockedMu.Unlock()
		}
	}()

	go func() {
		// TODO: This will run once on non graceful connection close. If c.connect() fails and returns error, it will
		// not be known and everything will hang forever (until manual c.Start()). So this should be handled properly.
		if err := <-closeCh; err != nil {
			_ = c.connect()
		}
	}()

	c.logger.Debug("Connection set up.")

	return nil
}

func (c *connection) disconnect() error {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	if c.conn == nil {
		c.logger.Debug("Not connected.")

		return nil
	}

	c.logger.Debug("Disconnecting.")

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}

	c.logger.Debug("Disconnected.")

	return nil
}

func (c *connection) newChannel() *channel {
	return &channel{
		retryStrategy: c.chRetryStrategy,
		channel:       nil,
		connection:    c,
		logger:        c.logger,
	}
}

func (c *connection) waitBlocked() {
	c.blockedMu.RLock()
	ch := c.blockedCh
	c.blockedMu.RUnlock()
	<-ch
}

type channel struct {
	retryStrategy RetryStrategy
	channel       *amqp.Channel
	connection    *connection
	logger        Logger
}

func (c *channel) connect() error {
	var (
		ch          *amqp.Channel
		channelErrs []error
	)

	c.logger.Debug("Setting up channel")

	for a := c.retryStrategy(); a.Attempt(); {
		var err error

		// If connection is closed, both connection and it's channels receive NotifyClose. For channel's close
		// notification chan there is no way to distinguish whether connection or channel was closed. So there's a
		// chance that channel will lock mutex earlier than connection will. But due to how RWMutex works, this should
		// consume at most one retry for each channel, so there is no reason to overengineer here ATM.
		ch, err = c.connection.channel()
		if err == nil {
			break
		}

		c.logger.Warn("Open channel error.", err)

		channelErrs = append(channelErrs, err)
	}

	if ch == nil {
		return fmt.Errorf("establish channel: %w", errors.Join(channelErrs...))
	}

	c.channel = ch

	c.logger.Debug("Channel set up.")

	return nil
}

func (c *channel) disconnect() error {
	if c.channel == nil {
		c.logger.Debug("Channel not connected.")

		return nil
	}

	c.logger.Debug("Disconnecting channel.")

	if err := c.channel.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}

	c.logger.Debug("Disconnected channel.")

	return nil
}

func (c *channel) waitBlocked() {
	c.connection.waitBlocked()
}
