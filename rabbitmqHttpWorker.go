package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ConfigParameters struct {
	QueueName      string
	RabbitmqURL    string
	PrefetchCount  int
	ConnRetryDelay int
	HttpTimeout    int
}

var config = ConfigParameters{}

type HttpRequestMessage struct {
	// RabbitMQ message
	delivery amqp.Delivery

	// Parsed fields
	url        string
	headers    map[string]string
	body       string
	expiration int64
	retry      int

	// Drop / Retry Indicator
	// Message is dropped after: Successful http request, message expiration, http response code 4XX or any other permanent error
	drop bool
}

var gracefulShutdown bool
var gracefulRestart bool
var connectionBroken bool

// Channel to receive asynchronous signals for graceful shutdown / restart
var signals chan os.Signal

func main() {
	usageMessage()

	runLoadConfig()

	// Register channel to receive OS signals
	// quit = graceful shutdown
	// hangup = graceful restart
	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGQUIT)

	for {
		consumeHttpRequests()

		if gracefulShutdown {
			if connectionBroken {
				log.Println("Broken connection to RabbitMQ was detected during shutdown")
			} else {
				log.Println("Graceful shutdown completed")
			}
			break
		}

		if connectionBroken {
			connectionBroken = false
			gracefulRestart = false
			log.Printf("Broken RabbitMQ connection was detected. Reconnect will be attempted in %d seconds...", config.ConnRetryDelay)
			time.Sleep(time.Duration(config.ConnRetryDelay) * time.Second)
		}

		if gracefulRestart {
			gracefulRestart = false
			log.Println("Restarting...")
		}

		runLoadConfig()
	}
}

func usageMessage() {
	help := make(map[string]bool)
	help["-h"] = true
	help["help"] = true
	help["-help"] = true
	help["--help"] = true

	argCnt := len(os.Args)

	if argCnt != 2 || help[os.Args[1]] == true {
		fmt.Println("Usage: rabbitmqHttpWorker CONFIG_FILE\n")
		os.Exit(1)
	}
}

func runLoadConfig() {
	log.Println("Loading the configuration file...")
	if err := loadConfig(); err != nil {
		log.Println("Could not load the configuration file:", err)
		os.Exit(1)
	}
	log.Println("Configuration file successfully loaded")
}

func loadConfig() error {
	configBytes, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		return errors.New("Error encountered reading file " + os.Args[1])
	}

	if err = json.Unmarshal(configBytes, &config); err != nil {
		return err
	}

	if len(config.QueueName) == 0 {
		return errors.New("QueueName is empty or missing")
	}

	if len(config.RabbitmqURL) == 0 {
		return errors.New("RabbitmqURL is empty or missing")
	}

	if config.PrefetchCount < 1 || config.PrefetchCount > 100 {
		return errors.New("PrefetchCount must be between 1 and 100")
	}

	if config.ConnRetryDelay < 10 || config.ConnRetryDelay > 300 {
		return errors.New("ConnRetryDelay must be between 10 and 300")
	}

	if config.HttpTimeout < 10 || config.HttpTimeout > 300 {
		return errors.New("HttpTimeout must be between 10 and 300")
	}

	return nil
}

func consumeHttpRequests() {
	log.Println("Connecting to RabbitMQ...")
	conn, err := amqp.Dial(config.RabbitmqURL)
	if err != nil {
		log.Println("Could not connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	log.Println("Connected successfully")

	log.Println("Opening a channel to RabbitMQ...")
	ch, err := conn.Channel()
	if err != nil {
		log.Println("Could not open a channel:", err)
		return
	}
	defer ch.Close()
	log.Println("Channel opened successfully")

	log.Println("Setting prefetch count on the channel...")
	if err = ch.Qos(config.PrefetchCount, 0, false); err != nil {
		log.Println("Could not set prefetch count:", err)
		return
	}
	log.Println("Set prefetch count successfully")

	log.Println("Registering a consumer...")
	deliveries, err := ch.Consume(
		config.QueueName, // queue
		"",               // consumer
		false,            // auto-ack
		false,            // exclusive
		false,            // no-local
		false,            // no-wait
		nil,              // args
	)
	if err != nil {
		log.Println("Could not register a consumer:", err)
		return
	}
	log.Println("Registered a consumer successfully")

	closedChannelListener := make(chan *amqp.Error, 1)
	ch.NotifyClose(closedChannelListener)
	log.Println("Started 'closed channel' listener")

	var msg HttpRequestMessage

	// Go channel to coordinate acknowledgment of RabbitMQ messages
	ackCh := make(chan HttpRequestMessage, config.PrefetchCount)

	unacknowledgedMsgs := 0

	for {
		select {
		// Process next available message from RabbitMQ
		case delivery := <-deliveries:
			unacknowledgedMsgs++
			log.Println("Unacknowledged message count:", unacknowledgedMsgs)
			log.Println("Message received from RabbitMQ. Parsing...")
			msg, err = parse(delivery)
			if err != nil {
				log.Println("Could not parse message:", err)
				msg.drop = true
				ackCh <- msg
			} else {
				log.Println("Message parsed successfully")
				go msg.httpPost(ackCh)
			}

		// Acknowledge RabbitMQ messages and indicate whether they should be dropped or retried
		case msg = <-ackCh:
			log.Println("Acknowledging message...")
			if err = msg.acknowledge(); err != nil {
				log.Println("Could not send message acknowledgement to RabbitMQ:", err)
				return
			}
			log.Println("Message acknowledged successfully")

			unacknowledgedMsgs--
			log.Println("Unacknowledged message count:", unacknowledgedMsgs)
			if unacknowledgedMsgs == 0 && (gracefulShutdown || gracefulRestart) {
				return
			}

		// Abort if a problem is detected with the RabbitMQ connection. The main() loop will attempt to reconnect.
		case <-closedChannelListener:
			connectionBroken = true
			return

		// Process request to gracefully shutdown / restart
		case sig := <-signals:
			switch signalName := sig.String(); signalName {
			case "hangup":
				log.Println("Graceful restart requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				gracefulRestart = true
				if unacknowledgedMsgs == 0 {
					return
				}
			case "quit":
				log.Println("Graceful shutdown requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				gracefulShutdown = true
				if unacknowledgedMsgs == 0 {
					return
				}
			}
		}
	}
}

func parse(rmqDelivery amqp.Delivery) (msg HttpRequestMessage, err error) {
	type MessageFields struct {
		Url        string
		Headers    []map[string]string
		Body       string
		Expiration int64
	}

	var fields MessageFields

	msg = HttpRequestMessage{delivery: rmqDelivery}

	if err := json.Unmarshal(rmqDelivery.Body, &fields); err != nil {
		return msg, err
	}

	// url
	if len(fields.Url) == 0 {
		err = errors.New("Field 'url' is empty or missing")
		return msg, err
	}
	msg.url = fields.Url

	// headers
	msg.headers = make(map[string]string)
	for _, m := range fields.Headers {
		for key, val := range m {
			msg.headers[key] = val
		}
	}

	// body
	msg.body = fields.Body

	// message expiration
	if fields.Expiration <= 0 {
		err = errors.New("Field 'expiration' is missing or invalid")
	}
	msg.expiration = fields.Expiration

	// Is this a retry? (as per RabbitMQ message headers)
	rmqHeaders := rmqDelivery.Headers
	if rmqHeaders != nil {
		deathHistory, ok := rmqHeaders["x-death"]
		if ok {
			// The RabbitMQ "death" history is provided as an array of 2 maps.  One map has the history for the wait queue, the other for the main queue.
			// The "count" field will have the same value in each map and it represents the # of times this message was dead-lettered to each queue.
			// As an example, if the count is currently two, then there have been two previous attempts to send this message and the upcoming attempt will be the 2nd retry.
			queueDeathHistory := deathHistory.([]interface{})
			if len(queueDeathHistory) == 2 {
				waitQueueDeathHistory := queueDeathHistory[0].(amqp.Table)
				retryCount, retryCountOk := waitQueueDeathHistory["count"]
				if retryCountOk {
					msg.retry = int(retryCount.(int64))
				}
			}
		}
	}

	log.Println("Parsed fields:", fields)
	log.Println("Retry:", msg.retry)

	return msg, nil
}

func (msg HttpRequestMessage) httpPost(ackCh chan HttpRequestMessage) {
	req, err := http.NewRequest("POST", msg.url, bytes.NewBufferString(msg.body))
	if err != nil {
		log.Println("Invalid http request:", err)
		msg.drop = true
		ackCh <- msg
		return
	}

	client := &http.Client{Timeout: time.Duration(config.HttpTimeout) * time.Second}

	for hkey, hval := range msg.headers {
		req.Header.Set(hkey, hval)
	}

	log.Println("Http POST Request url:", msg.url)
	resp, err := client.Do(req)

	if err != nil {
		log.Println("Error on http POST:", err)
		ackCh <- msg
		return
	} else {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println("Error encountered when reading POST response body:", err)
		} else {
			log.Println("POST response status code:", resp.StatusCode)
			log.Println("POST response body:", string(htmlData))
			resp.Body.Close()
		}
	}

	if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		log.Println("4XX error on http POST (no retry):", resp.Status)
		msg.drop = true
		ackCh <- msg
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		log.Println("Success on http POST:", resp.Status)
		msg.drop = true
		ackCh <- msg
		return
	}

	log.Println("Error on http POST:", resp.Status)
	ackCh <- msg
}

func (msg HttpRequestMessage) acknowledge() (err error) {
	if msg.drop {
		log.Println("Sending ACK (drop) for request to url:", msg.url)
		return msg.delivery.Ack(false)
	}

	// Should message be dropped because it expired?
	expired := time.Now().Unix() >= msg.expiration

	if expired {
		log.Println("Sending ACK (drop) for EXPIRED request to url:", msg.url)
		return msg.delivery.Ack(false)
	}

	log.Println("Sending NACK (retry) for request to url:", msg.url)
	return msg.delivery.Nack(false, false)
}
