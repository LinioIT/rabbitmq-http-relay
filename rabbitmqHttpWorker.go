package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/streadway/amqp"
	"log"
	"net/http"
	"time"
)

const (
	queueName      = "notifier"
	rabbitmqURL    = "amqp://rmq:rmq@localhost:5672/"
	prefetchCount  = 50 // Maximum number of unacknowledged messages allowed by RabbitMQ (maximum # of threads)
	connRetryDelay = 30 // Seconds to wait before attempting to restore a dropped RabbitMQ connection
	httpTimeout    = 30 // Timeout in seconds for each http request
)

type HttpRequestMessage struct {
	// RabbitMQ message
	delivery amqp.Delivery

	// Parsed fields
	url        string
	headers    map[string]string
	body       string
	expiration int64
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
	ackCh := make(chan HttpRequestMessage)

	for {
		select {

		// Process next available message from RabbitMQ
		case delivery := <-deliveries:
			msg, err = parse(delivery)
			if err != nil {
				log.Println("Could not parse message:", err)
				msg.processed = true
				ackCh <- msg
			} else {
				go msg.httpPost(ackCh)
			}

		// Acknowledge message status to RabbitMQ after the http request is processed.
		case msg = <-ackCh:
			if err = msg.acknowledge(); err != nil {
				log.Println("Could not send message acknowledgement to RabbitMQ:", err)
				return
			}

		// Abort if a problem is detected with the RabbitMQ connection. The main() loop will attempt to reconnect.
		case <-closedChannelListener:
			return
		}
	}
}

func parse(rmqDelivery amqp.Delivery) (msg HttpRequestMessage, err error) {
	type ParseFields struct {
		url        string
		body       string
		expiration int64
	}
	fields := ParseFields{}

	msg = HttpRequestMessage{delivery: rmqDelivery}

	err = json.Unmarshal(rmqDelivery.Body, &fields)
	if err == nil {
		if len(fields.url) == 0 {
			err = errors.New("Field 'url' is missing")
		} else if len(fields.body) == 0 {
			err = errors.New("Field 'body' is missing")
		} else if fields.expiration <= 0 {
			err = errors.New("Field 'expiration' is missing or invalid")
		} else {
			msg.url = fields.url
			msg.body = fields.body
			msg.expiration = fields.expiration
		}
	}

	return msg, err
}

func (msg HttpRequestMessage) httpPost(ackCh chan HttpRequestMessage) {
	req, _ := http.NewRequest("POST", msg.url, bytes.NewBufferString(msg.body))

	client := &http.Client{Timeout: time.Duration(httpTimeout) * time.Second}

	resp, err := client.Do(req)
	defer resp.Body.Close()

	if err != nil {
		log.Println("Error on http POST:", err)
		ackCh <- msg
		return
	}

	if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		log.Println("Fatal error on http POST:", resp.Status)
		msg.processed = true
		ackCh <- msg
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		msg.processed = true
		ackCh <- msg
		return
	}

	log.Println("Error on http POST:", resp.Status)
	ackCh <- msg
}

func (msg HttpRequestMessage) acknowledge() (err error) {
	if msg.processed {
		return msg.delivery.Ack(false)
	}

	expired := time.Now().Unix() >= msg.expiration

	if expired {
		return msg.delivery.Ack(false)
	} else {
		return msg.delivery.Nack(false, false)
	}
}
