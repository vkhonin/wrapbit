package transport

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/utils"
)

type Channel struct {
	CancelHandler func(tag string) error
	Ch            *amqp.Channel

	conn   *Connection
	logger utils.Logger
	retry  utils.Retry
}

func (c *Channel) Connect(ctx context.Context) error {
	var (
		ch   *amqp.Channel
		err  error
		errs []error
	)

	c.logger.Debug("Setting up channel")

	for a := c.retry(); a.Attempt(); {
		// If connection is closed, both connection and it's channels receive NotifyClose. For channel's close
		// notification chan there is no way to distinguish whether connection or channel was closed. So there's a
		// chance that channel will lock mutex earlier than connection will. But due to how RWMutex works, this should
		// consume at most one retry for each channel, so there is no reason to overengineer here ATM.
		if ch, err = c.conn.channel(); ch != nil {
			break
		}

		c.logger.Warn("Open channel error.", err)

		errs = append(errs, err)
	}

	if ch == nil {
		return fmt.Errorf("establish channel: %w", errors.Join(errs...))
	}

	c.Ch = ch

	c.handleAll(ctx)

	c.logger.Debug("Channel set up.")

	return nil
}

func (c *Channel) Disconnect() error {
	if c.Ch == nil {
		c.logger.Debug("Channel not connected.")

		return nil
	}

	c.logger.Debug("Disconnecting channel.")

	if err := c.Ch.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}

	c.logger.Debug("Disconnected channel.")

	return nil
}

func (c *Channel) WaitBlocked() {
	c.conn.waitBlocked()
}

func (c *Channel) handleAll(ctx context.Context) {
	var (
		cancelCh  <-chan string            = c.Ch.NotifyCancel(make(chan string, 1))
		closeCh   <-chan *amqp.Error       = c.Ch.NotifyClose(make(chan *amqp.Error, 1))
		flowCh    <-chan bool              = c.Ch.NotifyFlow(make(chan bool, 1))
		publishCh <-chan amqp.Confirmation = c.Ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		returnCh  <-chan amqp.Return       = c.Ch.NotifyReturn(make(chan amqp.Return, 1))
	)

	go c.handleCancel(ctx, cancelCh)
	go c.handleClose(ctx, closeCh)
	go c.handleFlow(ctx, flowCh)
	go c.handlePublish(ctx, publishCh)
	go c.handleReturn(ctx, returnCh)
}

func (c *Channel) handleCancel(_ context.Context, ch <-chan string) {
	for tag := range ch {
		if c.CancelHandler == nil {
			c.logger.Debug("Cancel handler not set.")

			continue
		}

		if err := c.CancelHandler(tag); err != nil {
			c.logger.Warn("Cancel handler error.", err)

			continue
		}

		c.logger.Debug("Cancel handled.", tag)
	}

	c.logger.Debug("Cancel handler stopped.")
}

func (c *Channel) handleClose(_ context.Context, ch <-chan *amqp.Error) {
	for err := range ch {
		c.logger.Debug("Close handled.", err)
	}

	c.logger.Debug("Close handler stopped.")
}

func (c *Channel) handleFlow(_ context.Context, ch <-chan bool) {
	for flow := range ch {
		c.logger.Debug("Flow handled.", flow)
	}

	c.logger.Debug("Flow handler stopped.")
}

func (c *Channel) handlePublish(_ context.Context, ch <-chan amqp.Confirmation) {
	for pub := range ch {
		c.logger.Debug("Publish handled.", pub)
	}

	c.logger.Debug("Publish handler stopped.")
}

func (c *Channel) handleReturn(_ context.Context, ch <-chan amqp.Return) {
	for ret := range ch {
		c.logger.Debug("Return handled.", ret)
	}

	c.logger.Debug("Return handler stopped.")
}
