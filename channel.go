package wrapbit

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"sync"
)

type channel struct {
	cancelHandler func(tag string) error
	chMu          *sync.RWMutex
	ch            *amqp.Channel
	conn          *connection
	logger        Logger
	retry         Retry
}

func (c *channel) connect(ctx context.Context) error {
	c.chMu.Lock()
	defer c.chMu.Unlock()

	if c.ch != nil {
		_ = c.ch.Close()
	}

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

	c.ch = ch

	c.handleAll(ctx)

	c.logger.Debug("Channel set up.")

	return nil
}

func (c *channel) disconnect() error {
	if c.ch == nil {
		c.logger.Debug("Channel not connected.")

		return nil
	}

	c.logger.Debug("Disconnecting channel.")

	if err := c.ch.Close(); err != nil {
		return fmt.Errorf("close channel: %w", err)
	}

	c.logger.Debug("Disconnected channel.")

	return nil
}

func (c *channel) publish(exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	// TODO: Should be replaced with local 'blocked' once 'handleBlock' implemented.
	c.conn.blockCond.L.Lock()
	for c.conn.blocked {
		c.logger.Warn("Publishing blocked.")
		c.conn.blockCond.Wait()
		c.logger.Debug("Publishing unblocked.")
	}
	c.conn.blockCond.L.Unlock()

	// TODO: We can lose some messages here, close notification likely to be received later than channel is actually
	// closed. In such a case, we need to repeat publishing. We either have to check for ErrClosed or for some kind
	// of our implementation of transient error. In any case, this should be done in Publisher, not in Channel, because
	// Publisher should be responsible on handling such errors.
	c.chMu.RLock()
	defer c.chMu.RUnlock()
	if err := c.ch.Publish(
		exchange,
		routingKey,
		mandatory,
		immediate,
		msg,
	); err != nil {
		return err
	}

	return nil
}

func (c *channel) handleAll(ctx context.Context) {
	var (
		cancelCh  <-chan string            = c.ch.NotifyCancel(make(chan string, 1))
		closeCh   <-chan *amqp.Error       = c.ch.NotifyClose(make(chan *amqp.Error, 1))
		flowCh    <-chan bool              = c.ch.NotifyFlow(make(chan bool, 1))
		publishCh <-chan amqp.Confirmation = c.ch.NotifyPublish(make(chan amqp.Confirmation, 1))
		returnCh  <-chan amqp.Return       = c.ch.NotifyReturn(make(chan amqp.Return, 1))
	)

	go c.handleCancel(ctx, cancelCh)
	go c.handleClose(ctx, closeCh)
	go c.handleFlow(ctx, flowCh)
	go c.handlePublish(ctx, publishCh)
	go c.handleReturn(ctx, returnCh)
}

func (c *channel) handleCancel(_ context.Context, ch <-chan string) {
	for tag := range ch {
		if c.cancelHandler == nil {
			c.logger.Debug("Cancel handler not set.")

			continue
		}

		if err := c.cancelHandler(tag); err != nil {
			c.logger.Warn("Cancel handler error.", err)

			continue
		}

		c.logger.Debug("Cancel handled.", tag)
	}

	c.logger.Debug("Cancel handler stopped.")
}

func (c *channel) handleClose(ctx context.Context, ch <-chan *amqp.Error) {
	err := <-ch
	if err == nil {
		return
	}

	c.logger.Warn("Channel closed with error.", err)

	if connErr := c.connect(ctx); connErr != nil {
		c.logger.Warn("Channel connection error.", connErr)
	}

	c.logger.Debug("Close handled.", err)
}

func (c *channel) handleFlow(_ context.Context, ch <-chan bool) {
	for flow := range ch {
		c.logger.Debug("Flow handled.", flow)
	}

	c.logger.Debug("Flow handler stopped.")
}

func (c *channel) handlePublish(_ context.Context, ch <-chan amqp.Confirmation) {
	for pub := range ch {
		c.logger.Debug("Publish handled.", pub)
	}

	c.logger.Debug("Publish handler stopped.")
}

func (c *channel) handleReturn(_ context.Context, ch <-chan amqp.Return) {
	for ret := range ch {
		c.logger.Debug("Return handled.", ret)
	}

	c.logger.Debug("Return handler stopped.")
}
