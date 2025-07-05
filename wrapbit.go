package wrapbit

import (
	"context"
	"errors"
	"fmt"
	"github.com/rabbitmq/amqp091-go"
	"log/slog"
	"slices"
	"sync"
	"time"
)

type Wrapbit struct {
	channel       *amqp091.Channel
	config        WrapbitConfig
	connectionMu  *sync.RWMutex
	connection    *amqp091.Connection
	exchanges     map[string]*Exchange
	logger        Logger
	publishers    map[string]*Publisher
	queueBindings map[string]*QueueBinding
	queues        map[string]*Queue
}

type WrapbitConfig struct {
	clusterURIs            []string
	channelRetries         int
	channelRetryTimeout    time.Duration
	connectionRetries      int
	connectionRetryTimeout time.Duration
}

type WrapbitOption func(w *Wrapbit) error

type Logger interface {
	Debug(args ...any)
	Error(args ...any)
	Info(args ...any)
	Warn(args ...any)
}

type logger struct{}

func (l *logger) Debug(args ...any) {
	l.log(context.Background(), slog.LevelDebug, args)
}

func (l *logger) Error(args ...any) {
	l.log(context.Background(), slog.LevelError, args)
}

func (l *logger) Info(args ...any) {
	l.log(context.Background(), slog.LevelInfo, args)
}

func (l *logger) Warn(args ...any) {
	l.log(context.Background(), slog.LevelWarn, args)
}

func (l *logger) log(ctx context.Context, level slog.Level, args ...any) {
	switch len(args) {
	case 0:
		slog.Log(ctx, level, "")
	case 1:
		slog.Log(ctx, level, fmt.Sprint(args[0]))
	default:
		slog.Log(ctx, level, fmt.Sprint(args[0]), args[1:])
	}
}

func NewInstance(options ...WrapbitOption) (*Wrapbit, error) {
	w := new(Wrapbit)
	w.config = wrapbitDefaultConfig()
	w.connectionMu = &sync.RWMutex{}
	w.exchanges = make(map[string]*Exchange)
	w.logger = new(logger)
	w.publishers = make(map[string]*Publisher)
	w.queueBindings = make(map[string]*QueueBinding)
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

// WithQueueBinding binds given queue to given exchange
func WithQueueBinding(queue, exchange string, options ...QueueBindingOption) WrapbitOption {
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

// WithChannelRetries sets number of failed channel opening attempts in case of error before stop attempts
func WithChannelRetries(n int) WrapbitOption {
	return func(w *Wrapbit) error {
		w.config.channelRetries = n

		return nil
	}
}

// WithChannelRetryTimeout sets timeout between failed channel opening attempts. Non-positive time.Duration means no
// timeout.
func WithChannelRetryTimeout(t time.Duration) WrapbitOption {
	return func(w *Wrapbit) error {
		if t.Nanoseconds() < 0 {
			t = 0
		}

		w.config.channelRetryTimeout = t

		return nil
	}
}

// WithConnectionRetries sets number failed connection attempts in case of error before stop attempts
func WithConnectionRetries(n int) WrapbitOption {
	return func(w *Wrapbit) error {
		w.config.connectionRetries = n

		return nil
	}
}

// WithConnectionRetryTimeout sets timeout between failed connection attempts. Non-positive time.Duration means no
// timeout.
func WithConnectionRetryTimeout(t time.Duration) WrapbitOption {
	return func(w *Wrapbit) error {
		if t.Nanoseconds() < 0 {
			t = 0
		}

		w.config.connectionRetryTimeout = t

		return nil
	}
}

// WithExchange declares given exchange
func WithExchange(name string, options ...ExchangeOption) WrapbitOption {
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

		// TODO: As queues can be declared without name (it will be assigned by server on declaration), there will be
		// conflict below. We need some logic to:
		// 1. Store nameless queues before actual declaration.
		// 2. Update their mappings after declaration, when the actual name will be assigned by the server.
		w.queues[name] = q

		return nil
	}
}

func (w *Wrapbit) Start() error {
	if err := w.connect(); err != nil {
		return err
	}

	ch, err := w.newChannel()
	if err != nil {
		return fmt.Errorf("establish channel: %w", err)
	}

	w.channel = ch

	for _, q := range w.queues {
		c := &q.config
		_, err = w.channel.QueueDeclare(c.name, c.durable, c.autoDelete, c.exclusive, c.noWait, c.args)
		if err != nil {
			return fmt.Errorf("declare queue: %w", err)
		}
	}

	for _, e := range w.exchanges {
		c := &e.config
		err = w.channel.ExchangeDeclare(c.name, c.kind, c.durable, c.autoDelete, c.internal, c.noWait, c.args)
		if err != nil {
			return fmt.Errorf("declare exchange: %w", err)
		}
	}

	for _, b := range w.queueBindings {
		c := &b.config
		err = w.channel.QueueBind(c.name, c.key, c.exchange, c.noWait, c.args)
		if err != nil {
			return fmt.Errorf("binding queue: %w", err)
		}
	}

	return nil
}

func (w *Wrapbit) Stop() error {
	w.connectionMu.RLock()
	defer w.connectionMu.RUnlock()

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

func (w *Wrapbit) connect() error {
	w.connectionMu.Lock()
	defer w.connectionMu.Unlock()

	var connErrs []error

connection:
	for range w.config.connectionRetries {
		for _, uri := range w.config.clusterURIs {
			conn, err := amqp091.Dial(uri)
			if err != nil {
				connErrs = append(connErrs, err)

				continue
			}

			w.connection = conn

			break connection
		}

		if w.config.connectionRetryTimeout.Nanoseconds() > 0 {
			time.Sleep(w.config.connectionRetryTimeout)
		}
	}

	if w.connection == nil {
		var err error
		if len(w.config.clusterURIs) == 0 {
			err = errors.New("no URIs to connect")
		} else {
			err = errors.Join(connErrs...)
		}

		return fmt.Errorf("establish connection: %w", err)
	}

	var connCloseChan <-chan *amqp091.Error = w.connection.NotifyClose(make(chan *amqp091.Error, 1))

	go func() {
		// TODO: This will run once on non graceful connection close. If w.connect() fails and returns error, it will
		// not be known and everything will hang forever (until manual w.Start()). So this should be handled properly.
		if err := <-connCloseChan; err != nil {
			_ = w.connect()
		}
	}()

	return nil
}

func (w *Wrapbit) newChannel() (*amqp091.Channel, error) {
	var (
		channel     *amqp091.Channel
		channelErrs []error
	)

	for range w.config.connectionRetries {
		var err error

		// If connection is closed, both connection and it's channels receive NotifyClose. For channel's close
		// notification chan there is no way to distinguish whether connection or channel was closed. So there's a
		// chance that channel will lock mutex earlier than connection will. But due to how RWMutex works, this should
		// consume at most one retry for each channel, so there is no reason to overengineer here ATM.
		w.connectionMu.RLock()
		channel, err = w.connection.Channel()
		w.connectionMu.RUnlock()
		if err == nil {
			break
		}

		channelErrs = append(channelErrs, err)

		if w.config.channelRetryTimeout.Nanoseconds() > 0 {
			time.Sleep(w.config.channelRetryTimeout)
		}
	}

	if channel == nil {
		return nil, fmt.Errorf("establish channel: %w", errors.Join(channelErrs...))
	}

	return channel, nil
}
