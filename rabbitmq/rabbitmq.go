package rabbitmq

import (
	"errors"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/LinioIT/rabbitmq-worker/message"
	"github.com/streadway/amqp"
	"time"
)

type RMQConnection struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

type Delivery amqp.Delivery

// QueueCheck creates/verifies RabbitMQ queues
func QueueCheck(config *config.ConfigParameters) error {
	waitQueue := config.Queue.Name + "_wait"

	conn, err := amqp.Dial(config.Connection.RabbitmqURL)
	if err != nil {
		return errors.New("Could not connect to RabbitMQ")
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return errors.New("Could not open a RabbitMQ channel")
	}
	defer ch.Close()

	// Create main queue
	args := make(amqp.Table)
	args["x-dead-letter-exchange"] = ""
	args["x-dead-letter-routing-key"] = waitQueue
	_, err = ch.QueueDeclare(
		config.Queue.Name, // name
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		args,              // arguments
	)
	if err != nil {
		return errors.New("Could not declare queue " + config.Queue.Name)
	}

	// Create wait queue with dead-lettering back to main queue
	args = make(amqp.Table)
	args["x-message-ttl"] = 1000 * int32(config.Queue.WaitDelay)
	args["x-dead-letter-exchange"] = ""
	args["x-dead-letter-routing-key"] = config.Queue.Name
	_, err = ch.QueueDeclare(
		waitQueue, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	if err != nil {
		return errors.New("Could not declare queue " + waitQueue)
	}

	return nil
}

func (rmq *RMQConnection) Open(config *config.ConfigParameters, logFile *logfile.Logger) (<-chan amqp.Delivery, <-chan *amqp.Error, error) {
	var deliveries <-chan amqp.Delivery
	closedChannelListener := make(chan *amqp.Error, 1)
	var err error

	logFile.Write("Connecting to RabbitMQ...")
	rmq.conn, err = amqp.Dial(config.Connection.RabbitmqURL)
	if err != nil {
		rmqErr := errors.New("Could not connect to RabbitMQ: " + err.Error())
		return deliveries, closedChannelListener, rmqErr
	}
	logFile.Write("Connected successfully")

	logFile.Write("Opening a channel to RabbitMQ...")
	rmq.ch, err = rmq.conn.Channel()
	if err != nil {
		rmqErr := errors.New("Could not open a channel: " + err.Error())
		return deliveries, closedChannelListener, rmqErr
	}
	logFile.Write("Channel opened successfully")

	logFile.Write("Setting prefetch count on the channel to", config.Queue.PrefetchCount, "...")
	if err = rmq.ch.Qos(config.Queue.PrefetchCount, 0, false); err != nil {
		rmqErr := errors.New("Could not set prefetch count: " + err.Error())
		return deliveries, closedChannelListener, rmqErr
	}
	logFile.Write("Prefetch count set successfully")

	logFile.Write("Registering a consumer...")
	deliveries, err = rmq.ch.Consume(
		config.Queue.Name, // queue
		"",                // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		rmqErr := errors.New("Could not register the consumer: " + err.Error())
		return deliveries, closedChannelListener, rmqErr
	}
	logFile.Write("Consumer registered successfully")

	rmq.ch.NotifyClose(closedChannelListener)
	logFile.Write("Started 'closed channel' listener")

	return deliveries, closedChannelListener, nil
}

func (rmq *RMQConnection) Close() {
	if rmq.ch != nil {
		rmq.ch.Close()
	}

	if rmq.conn != nil {
		rmq.conn.Close()
	}
}

func Acknowledge(msg message.HttpRequestMessage, config *config.ConfigParameters, logFile *logfile.Logger) (err error) {
	if msg.Drop {
		logFile.Write("Dropping Message ID", msg.MessageId)
		logFile.WriteDebug("Sending ACK (drop) for Message ID", msg.MessageId)
		return msg.Delivery.Ack(false)
	}

	// Drop message if it will expire before the next retry
	expired := false
	if msg.Expiration > 0 {
		// Use the expiration time included with the message, if one was provided
		expired = msg.Expiration < (time.Now().Unix() + int64(config.Queue.WaitDelay))
	} else {
		// Otherwise, compare the default TTL to the time the message was first rejected
		if msg.FirstRejectionTime > 0 {
			expired = (msg.FirstRejectionTime + int64(config.Message.DefaultTTL)) < (time.Now().Unix() + int64(config.Queue.WaitDelay))
		}
	}

	if expired {
		logFile.Write("Message ID", msg.MessageId, "has EXPIRED")
		logFile.WriteDebug("Sending ACK (drop) for EXPIRED Message ID", msg.MessageId)
		return msg.Delivery.Ack(false)
	}

	logFile.Write("Message ID", msg.MessageId, "will be retried")
	logFile.WriteDebug("Sending NACK (retry) for Message ID", msg.MessageId)
	return msg.Delivery.Nack(false, false)
}
