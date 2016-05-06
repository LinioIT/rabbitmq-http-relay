package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
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

	headers := make(amqp.Table)

	var url string
	if len(os.Args) > 1 {
		url = os.Args[1]
	} else {
		url = "http://httpbin.org/post"
	}

	body := `{"url": "` + url + `", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "{\"key\": \"1230789\"}"}`

	if len(os.Args) > 2 {
		headers["message_id"] = os.Args[2]
	}

	if len(os.Args) > 3 {
		msg_ttl, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err == nil {
			headers["expiration"] = int64(time.Now().Unix()) + msg_ttl
		}
	}

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
	log.Println("Published the http request message")
	failOnError(err, "Failed to publish the http request message")
}
