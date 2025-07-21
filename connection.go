package wrapbit

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type connection struct {
	blockCond *sync.Cond
	blocked   bool
	chRetry   Retry
	connMu    sync.RWMutex
	conn      *amqp.Connection
	logger    Logger
	retry     Retry
	uris      []string
}

func (c *connection) connect(ctx context.Context) error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.conn != nil {
		_ = c.conn.Close()
	}

	c.logger.Debug("Setting up connection.")

	if err := c.dial(); err != nil {
		return fmt.Errorf("establish connection: %w", err)
	}

	c.logger.Debug("Setting up connection notifications.")

	var (
		blockCh <-chan amqp.Blocking = c.conn.NotifyBlocked(make(chan amqp.Blocking, 1))
		closeCh <-chan *amqp.Error   = c.conn.NotifyClose(make(chan *amqp.Error, 1))
	)

	go c.handleBlock(ctx, blockCh)
	go c.handleClose(ctx, closeCh)

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
		chMu:   new(sync.RWMutex),
		conn:   c,
		logger: c.logger,
		retry:  c.chRetry,
	}
}

func (c *connection) block(value bool) {
	c.blockCond.L.Lock()
	c.blocked = value
	c.blockCond.L.Unlock()
	c.blockCond.Broadcast()
}

func (c *connection) channel() (*amqp.Channel, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn.Channel()
}

// dial tries to establish connection to server using [Connection.Retry] strategy.
func (c *connection) dial() error {
	if len(c.uris) == 0 {
		return errors.New("no URIs to connect")
	}

	var (
		conn *amqp.Connection
		err  error
		errs []error
	)

	for a := c.retry(); a.Attempt(); {
		for _, uri := range c.uris {
			c.logger.Debug("Dialing.", uri)

			conn, err = amqp.Dial(uri)

			if err == nil {
				c.conn = conn

				return nil
			}

			c.logger.Warn("Dial error.", uri, err)

			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (c *connection) handleBlock(_ context.Context, ch <-chan amqp.Blocking) {
	c.block(false)

	for blocking := range ch {
		c.logger.Warn("Connection (un)block.", blocking)
		c.block(blocking.Active)
	}
}

func (c *connection) handleClose(ctx context.Context, ch <-chan *amqp.Error) {
	err := <-ch
	if err == nil {
		return
	}

	c.logger.Warn("Connection closed with error.", err)

	if connErr := c.connect(ctx); connErr != nil {
		c.logger.Warn("Connection establish error.", connErr)
	}

	c.logger.Debug("Connection close handled.", err)
}
