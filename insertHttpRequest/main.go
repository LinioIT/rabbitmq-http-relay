package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"strconv"
	"time"
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

	expiration := int(time.Now().Unix() + 85)
	// body := `{"url": "http://httpbin.org/post", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "{\"key\": \"1230789\"}"}`

	// body := `{"url": "http://fake-response.appspot.com/?sleep=15", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "ok"}`

	body := `{"url": "http://httpbin.org/status/501", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "ok"}`

	// body := `{"url": "http://localhost/pause.php?delay=20", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "ok"}'

	// body := `{"url": "http://localhost/redirect.php?delay=7", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "ok}"

	headers := make(amqp.Table)
	headers["expiration"] = strconv.Itoa(expiration)

	for i := 0; i < 1; i++ {
		err = ch.Publish(
			"",         // exchange
			"notifier", // routing key
			false,      // mandatory
			false,      // immediate
			amqp.Publishing{
				Headers:      headers,
				ContentType:  "application/json",
				DeliveryMode: uint8(persistentDeliveryMode),
				Body:         []byte(body),
			})
		log.Printf(" [x] Sent %s", body)
		failOnError(err, "Failed to publish a message")
	}
}
