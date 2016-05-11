package main

import (
	"flag"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
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

	// Get command line flags
	var url string
	var method string
	var headers string
	var body string
	var id string
	var ttl int64

	flag.StringVar(&url, "url", "", "http url")
	flag.StringVar(&method, "method", "", "http request method")
	flag.StringVar(&headers, "headers", `[{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}]`, "http request headers")
	flag.StringVar(&body, "body", `{\"key\": \"1230789\"}`, "http request body")
	flag.StringVar(&id, "id", "", "message id")
	flag.Int64Var(&ttl, "ttl", 0, "message ttl")

	flag.Usage = usage
	flag.Parse()

	if len(url) == 0 {
		usage()
	}

	messageBody := `{"url": "` + url + `"`
	if len(method) > 0 {
		messageBody += `, "method": "`
		messageBody += method
		messageBody += `"`
	}
	if len(headers) > 0 {
		messageBody += `, "headers": `
		messageBody += headers
	}
	if len(body) > 0 {
		messageBody += `, "body": "`
		messageBody += body
		messageBody += `"`
	}
	messageBody += `}`

	messageHeaders := make(amqp.Table)

	if len(id) > 0 {
		messageHeaders["message_id"] = id
	}

	if ttl > 0 {
		messageHeaders["expiration"] = int64(time.Now().Unix()) + ttl
	}

	err = ch.Publish(
		"",         // exchange
		"notifier", // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			Headers:      messageHeaders,
			ContentType:  "application/json",
			DeliveryMode: uint8(persistentDeliveryMode),
			Body:         []byte(messageBody),
		})
	failOnError(err, "Failed to publish the http request message!")

	fmt.Println("Published the http request message...")
	fmt.Println("Message Body:", messageBody)
	if len(id) > 0 {
		fmt.Println("Message ID:", id)
	}
	if ttl > 0 {
		fmt.Println("Message TTL:", ttl)
	}
	fmt.Println("")
}

func usage() {
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Usage:", os.Args[0], "--url=URL [OPTION]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  --url            HTTP request URL. Required.")
	fmt.Fprintln(os.Stderr, "  --method         HTTP request method. If not provided, the default method will be used.")
	fmt.Fprintln(os.Stderr, "  --headers        HTTP request headers - JSON encoded array. Defaults to sample headers.")
	fmt.Fprintln(os.Stderr, "  --body           HTTP request body. Defaults to a sample JSON body. Double-quotes in JSON must be escaped.")
	fmt.Fprintln(os.Stderr, "  --id             Message ID. If not provided, an id will be generated.")
	fmt.Fprintln(os.Stderr, "  --ttl            Message TTL. If not provided, the default expiration will be used.")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example:", os.Args[0], `--url=http://httpbin.org/post --method=POST --headers='[{"Content-Type": "application/json"}]' --body='{\"key\": \"1230789\"}' --id=MSGID00002 --ttl=55`)
	fmt.Fprintln(os.Stderr)
	os.Exit(1)
}
