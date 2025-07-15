package transport

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/utils"
	"sync"
)

type Connection struct {
	BlockChan chan struct{}
	ChRetry   utils.Retry
	Logger    utils.Logger
	Retry     utils.Retry
	URIs      []string

	blockMu sync.RWMutex
	connMu  sync.RWMutex
	conn    *amqp.Connection
}

func (c *Connection) channel() (*amqp.Channel, error) {
	c.connMu.RLock()
	defer c.connMu.RUnlock()
	return c.conn.Channel()
}

func (c *Connection) Connect() error {
	c.Logger.Debug("Setting up connection.")

	c.connMu.Lock()
	defer c.connMu.Unlock()

	conn, err := c.dial()
	if err != nil {
		return fmt.Errorf("establish connection: %w", err)
	}

	c.conn = conn

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

func (c *Connection) dial() (*amqp.Connection, error) {
	if len(c.URIs) == 0 {
		return nil, errors.New("no URIs to connect")
	}

	var (
		conn *amqp.Connection
		err  error
		errs []error
	)

	for a := c.Retry(); a.Attempt(); {
		for _, uri := range c.URIs {
			c.Logger.Debug("Dialing.", uri)

			if conn, err = amqp.Dial(uri); err == nil {
				return conn, nil
			}

			c.Logger.Warn("Dial error.", uri, err)

			errs = append(errs, err)
		}
	}

	return nil, errors.Join(errs...)
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

func (c *Connection) handleBlock(blockCh <-chan amqp.Blocking) {
	// Closes blockChan if not closed yet, so that waiting instances will not hang forever.
	c.blockMu.Lock()
	select {
	case <-c.BlockChan:
	default:
		if c.BlockChan != nil {
			close(c.BlockChan)
		}
	}
	c.blockMu.Unlock()

	for blocking := range blockCh {
		c.blockMu.Lock()
		if blocking.Active {
			c.BlockChan = make(chan struct{})
		} else {
			close(c.BlockChan)
		}
		c.blockMu.Unlock()
	}
}

func (c *Connection) handleClose(closeCh <-chan *amqp.Error) {
	// TODO: This will run once on non graceful connection close. If c.connect() fails and returns error, it will
	// not be known and everything will hang forever (until manual c.Start()). So this should be handled properly.
	if err := <-closeCh; err != nil {
		_ = c.Connect()
	}
}

func (c *Connection) NewChannel() *Channel {
	return &Channel{
		conn:   c,
		logger: c.Logger,
		retry:  c.ChRetry,
	}
}

func (c *Connection) waitBlocked() {
	c.blockMu.RLock()
	ch := c.BlockChan
	c.blockMu.RUnlock()
	<-ch
}
