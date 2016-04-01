package main

import (
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strconv"
)

const (
	rabbitmqConn = "amqp://rmq:rmq@localhost:5672/"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	usageMessage()

	queue, retryDelay, err := getQueueDetails()
	// queue, retryDelay, err := getQueueDetails()
	failOnError(err, "Invalid command line arguments")

	waitQueue := queue + "_wait"

	conn, err := amqp.Dial(rabbitmqConn)
	failOnError(err, "Could not connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Could not open a RabbitMQ channel")
	defer ch.Close()

	// Create main queue
	_, err = ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Could not declare queue "+queue)

	// Create wait queue with dead-lettering back to main queue
	args := make(amqp.Table)
	args["x-message-ttl"] = int32(retryDelay)
	args["x-dead-letter-exchange"] = ""
	args["x-dead-letter-routing-key"] = queue
	_, err = ch.QueueDeclare(
		waitQueue, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		args,      // arguments
	)
	failOnError(err, "Could not declare queue "+waitQueue)

	log.Printf("Queues created successfully: %s, %s\n", queue, waitQueue)
}

func usageMessage() {
	help := make(map[string]bool)
	help["-h"] = true
	help["help"] = true
	help["-help"] = true
	help["--help"] = true

	argCnt := len(os.Args)

	if argCnt == 1 || (argCnt == 2 && help[os.Args[1]] == true) {
		fmt.Println("Usage: create_queues QUEUE_NAME RETRY_DELAY_SECS\n")
		os.Exit(1)
	}
}

func getQueueDetails() (string, int, error) {
	if len(os.Args) != 3 {
		return "", 0, errors.New("Incorrect number of arguments")
	}

	if len(os.Args[1]) < 3 || len(os.Args[1]) > 30 {
		return "", 0, errors.New("Queue Name must be between between 3 and 30 characters")
	}

	delay, err := strconv.ParseInt(os.Args[2], 10, 32)
	if err != nil || delay < 30 || delay > 3600 {
		return "", 0, errors.New("Retry Delay Seconds must be between 30 and 3600")
	}

	return os.Args[1], int(delay * 1000), nil
}
