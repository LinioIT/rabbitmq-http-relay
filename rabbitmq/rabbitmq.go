package rabbitmq

import (
	"errors"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/LinioIT/rabbitmq-worker/message"
	"github.com/streadway/amqp"
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

// QueueDelete removes RabbitMQ queues if they are not currently in use
func QueueDelete(config *config.ConfigParameters) error {
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

	// Delete main queue
	_, err = ch.QueueDelete(
		config.Queue.Name, // name
		true,              // if unused
		true,              // if empty
		false,             // no wait
	)
	if err != nil {
		return errors.New("Could not delete queue " + config.Queue.Name + " - " + err.Error())
	}

	// Delete wait queue
	_, err = ch.QueueDelete(
		waitQueue, // name
		true,      // if unused
		true,      // if empty
		false,     // no wait
	)
	if err != nil {
		return errors.New("Could not delete queue " + waitQueue + " - " + err.Error())
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

	logFile.Write("Setting prefetch count to", config.Queue.PrefetchCount, "for the channel...")
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

func Acknowledge(msg message.HttpRequestMessage, logFile *logfile.Logger) (err error) {
	if msg.Expired {
		logFile.Write("Message ID", msg.MessageId, "- EXPIRED")
		logFile.WriteDebug("Message ID", msg.MessageId, "- Sending ACK (drop) for EXPIRED message")
		return msg.Delivery.Ack(false)
	}

	if msg.Drop {
		logFile.Write("Message ID", msg.MessageId, "- Dropping")
		logFile.WriteDebug("Message ID", msg.MessageId, "- Sending ACK (drop)")
		return msg.Delivery.Ack(false)
	}

	logFile.Write("Message ID", msg.MessageId, "- Queueing for retry")
	logFile.WriteDebug("Message ID", msg.MessageId, "- Sending NACK (retry)")
	return msg.Delivery.Nack(false, false)
}

func GetDeliveryChan(size int) <-chan amqp.Delivery {
	return make(chan amqp.Delivery, size)
}
