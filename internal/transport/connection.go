package transport

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/utils"
	"sync"
)

type Connection struct {
	ChRetry utils.Retry
	Logger  utils.Logger
	Retry   utils.Retry
	URIs    []string

	BlockCond *sync.Cond
	blocked   bool
	connMu    sync.RWMutex
	conn      *amqp.Connection
}

func (c *Connection) Connect() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	c.Logger.Debug("Setting up connection.")

	if err := c.dial(); err != nil {
		return fmt.Errorf("establish connection: %w", err)
	}

	c.Logger.Debug("Setting up connection notifications.")

	var (
		blockCh <-chan amqp.Blocking = c.conn.NotifyBlocked(make(chan amqp.Blocking, 1))
		closeCh <-chan *amqp.Error   = c.conn.NotifyClose(make(chan *amqp.Error, 1))
	)

	go c.handleBlock(blockCh)
	go c.handleClose(closeCh)

	c.Logger.Debug("Connection set up.")

	return nil
}

func (c *Connection) Disconnect() error {
	c.connMu.RLock()
	defer c.connMu.RUnlock()

	if c.conn == nil {
		c.Logger.Debug("Not connected.")

		return nil
	}

	c.Logger.Debug("Disconnecting.")

	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close connection: %w", err)
	}

	c.Logger.Debug("Disconnected.")

	return nil
}

func (c *Connection) NewChannel() *Channel {
	return &Channel{
		chMu:   new(sync.RWMutex),
		conn:   c,
		logger: c.Logger,
		retry:  c.ChRetry,
	}
}

func (c *Connection) block(value bool) {
	c.BlockCond.L.Lock()
	c.blocked = value
	c.BlockCond.L.Unlock()
	c.BlockCond.Broadcast()
}

func (c *Connection) channel() (*amqp.Channel, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn.Channel()
}

// dial tries to establish connection to server using [Connection.Retry] strategy.
func (c *Connection) dial() error {
	if len(c.URIs) == 0 {
		return errors.New("no URIs to connect")
	}

	var (
		conn *amqp.Connection
		err  error
		errs []error
	)

	for a := c.Retry(); a.Attempt(); {
		for _, uri := range c.URIs {
			c.Logger.Debug("Dialing.", uri)

			conn, err = amqp.Dial(uri)

			if err == nil {
				c.conn = conn

				return nil
			}

			c.Logger.Warn("Dial error.", uri, err)

			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

func (c *Connection) handleBlock(blockCh <-chan amqp.Blocking) {
	c.block(false)

	for blocking := range blockCh {
		c.Logger.Warn("Connection (un)block.", blocking)
		c.block(blocking.Active)
	}
}

func (c *Connection) handleClose(closeCh <-chan *amqp.Error) {
	// TODO: This will run once on non graceful connection close. If c.connect() fails and returns error, it will
	// not be known and everything will hang forever (until manual c.Start()). So this should be handled properly.
	if err := <-closeCh; err != nil {
		c.Logger.Warn("Connection closed.", err)
		_ = c.Connect()
	}
}
