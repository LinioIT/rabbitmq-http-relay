package main

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

const (
	queueName      = "notifier"
	rabbitmqURL    = "amqp://rmq:rmq@localhost:5672/"
	prefetchCount  = 50    // Maximum number of unacknowledged messages allowed by RabbitMQ (maximum # of threads)
	connRetryDelay = 30    // Seconds to wait before attempting to restore a dropped RabbitMQ connection
	messageTTL     = 86400 // Total seconds before an unsuccessfully processed message is dropped
	httpTimeout    = 30    // Timeout in seconds for each http request
)

type HttpRequestMessage struct {
	// RabbitMQ message
	delivery amqp.Delivery

	// Parsed fields
	url        string
	headers    map[string]string
	body       string
	expiration int
	retries    int

	// Http Request Status
	processed bool
}

func main() {
	for {
		consumeHttpRequests()

		log.Println("Lost connection to RabbitMQ")

		time.Sleep(connRetryDelay * time.Second)
	}
}

func consumeHttpRequests() {
	log.Println("Connecting to RabbitMQ...")
	conn, err := amqp.Dial(rabbitmqURL)
	if err != nil {
		log.Println("Could not connect to RabbitMQ:", err)
		return
	}
	log.Println("Connected successfully")
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Println("Could not open a channel:", err)
		return
	}
	defer ch.Close()

	if err = ch.Qos(prefetchCount, 0, false); err != nil {
		log.Println("Could not set prefetch count on channel:", err)
		return
	}

	deliveries, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)
	if err != nil {
		log.Println("Could not register a consumer:", err)
		return
	}

	closedChannelListener := make(chan *amqp.Error)
	ch.NotifyClose(closedChannelListener)

	var msg HttpRequestMessage
	statusCh := make(chan HttpRequestStatus)
	for {
		select {
		// Process next available message from RabbitMQ
		case delivery := <-deliveries:
			msg = parseMessage(delivery)
			go httpRequest(&msg, statusCh)

		// Acknowledge message status to RabbitMQ after the http request is processed.
		// Ack() is sent on success and RabbitMQ will drop the message.
		// Nack() is sent on failure and RabbitMQ will dead-letter the message to the wait queue*.
		// *NOTE: If the message TTL has been reached then Ack() is sent, so that RabbitMQ drops the message.
		case msg = <-statusCh:
			acknowledgeMessage(&msg)

		// Abort if a problem is detected with the RabbitMQ connection. The main() loop will attempt to reconnect.
		case <-closedChannelListener:
			return
		}
	}
}
