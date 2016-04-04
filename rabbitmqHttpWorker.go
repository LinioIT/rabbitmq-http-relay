package main

import (
	"github.com/streadway/amqp"
	"log"
	"time"
)

const (
	queueName     = "notifier"
	rabbitmqURL   = "amqp://rmq:rmq@localhost:5672/"
	connRetrySecs = 30
	// httpRequestThreads = 50
)

func main() {
	for {
		consumeHttpRequests()

		log.Println("Lost connection to RabbitMQ")

		time.Sleep(connRetrySecs * time.Second)
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

	msgs, err := ch.Consume(
		queueName, // queue
		"",        // consumer
		true,      // auto-ack
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

	for {
		select {
		case httpReq := <-msgs:
			log.Printf("Received a message: %s", httpReq.Body)
		case <-closedChannelListener:
			return
		}
	}
}
