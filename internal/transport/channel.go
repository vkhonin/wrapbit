package transport

import (
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/vkhonin/wrapbit/utils"
)

type Channel struct {
	Ch *amqp.Channel

	conn   *Connection
	logger utils.Logger
	retry  utils.Retry
}

func (c *Channel) Connect() error {
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
