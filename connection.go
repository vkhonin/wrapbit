package wrapbit

import (
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

func (c *connection) connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	c.logger.Debug("Setting up connection.")

	if err := c.dial(); err != nil {
		return fmt.Errorf("establish connection: %w", err)
	}

	c.logger.Debug("Setting up connection notifications.")

	var (
		blockCh <-chan amqp.Blocking = c.conn.NotifyBlocked(make(chan amqp.Blocking, 1))
		closeCh <-chan *amqp.Error   = c.conn.NotifyClose(make(chan *amqp.Error, 1))
	)

	go c.handleBlock(blockCh)
	go c.handleClose(closeCh)

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

func (c *connection) handleBlock(blockCh <-chan amqp.Blocking) {
	c.block(false)

	for blocking := range blockCh {
		c.logger.Warn("Connection (un)block.", blocking)
		c.block(blocking.Active)
	}
}

func (c *connection) handleClose(closeCh <-chan *amqp.Error) {
	// TODO: This will run once on non graceful connection close. If c.connect() fails and returns error, it will
	// not be known and everything will hang forever (until manual c.Start()). So this should be handled properly.
	if err := <-closeCh; err != nil {
		c.logger.Warn("Connection closed.", err)
		_ = c.connect()
	}
}
