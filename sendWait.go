package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
)

const persistentDeliveryMode = 2

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func main() {
	conn, err := amqp.Dial("amqp://rmq:rmq@localhost:5672/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	/*
		q, err := ch.QueueDeclare(
			"notifier_wait", // name
			false,           // durable
			false,           // delete when unused
			false,           // exclusive
			false,           // no-wait
			nil,             // arguments
		)
		failOnError(err, "Failed to declare a queue")
	*/

	body := "hello"
	err = ch.Publish(
		"",              // exchange
		"notifier_wait", // routing key
		false,           // mandatory
		false,           // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			DeliveryMode: uint8(persistentDeliveryMode),
			Body:         []byte(body),
		})
	log.Printf(" [x] Sent %s", body)
	failOnError(err, "Failed to publish a message")
}
