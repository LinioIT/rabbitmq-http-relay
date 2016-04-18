package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"gopkg.in/gcfg.v1"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type LogLevel int

const (
	Fatal LogLevel = iota
	Warn
	Info
	Debug
)

var logLevels = [...]string{
	"Fatal",
	"Warn",
	"Info",
	"Debug",
}

func logPrintln(level LogLevel, args ...interface{}) {
	if int(level) <= config.Log.LevelInt {
		log.Println(args...)
	}
}

type ConfigParameters struct {
	Connection struct {
		RabbitmqURL string
		RetryDelay  int
	}
	Queue struct {
		Name          string
		WaitDelay     int
		PrefetchCount int
	}
	Message struct {
		DefaultTTL int
	}
	Http struct {
		Timeout int
	}
	Log struct {
		Level    string
		LevelInt int
	}
}

var config = ConfigParameters{}

type HttpRequestMessage struct {
	// RabbitMQ message
	delivery amqp.Delivery

	// Http request fields
	url     string
	headers map[string]string
	body    string

	// Time when message was originally created (if timestamp plugin was installed)
	timestamp int64

	// Time when message will expire (if not provided, value is calculated from DefaultTTL setting)
	expiration int64

	// Retry history from RabbitMQ headers
	retry              int
	firstRejectionTime int64

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

	// Start listener for OS signals
	// quit = graceful shutdown
	// hangup = graceful restart
	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGQUIT)

	for {
		runLoadConfig()

		runQueueCheck()

		consumeHttpRequests()

		if gracefulShutdown {
			if connectionBroken {
				logPrintln(Fatal, "Broken connection to RabbitMQ was detected during shutdown")
			} else {
				logPrintln(Info, "Graceful shutdown completed")
			}
			break
		}

		if connectionBroken {
			connectionBroken = false
			gracefulRestart = false
			logPrintln(Warn, "Broken RabbitMQ connection was detected. Reconnect will be attempted in", config.Connection.RetryDelay, "seconds...")
			time.Sleep(time.Duration(config.Connection.RetryDelay) * time.Second)
		}

		if gracefulRestart {
			gracefulRestart = false
			logPrintln(Info, "Restarting...")
		}
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
	if err := loadConfig(); err != nil {
		logPrintln(Fatal, "Could not load the configuration file:", err)
		os.Exit(1)
	}
	logPrintln(Info, "Configuration file successfully loaded")
	logPrintln(Debug, "config:", config)
}

func loadConfig() error {
	configBytes, err := ioutil.ReadFile(os.Args[1])
	if err != nil {
		return errors.New("Error encountered reading file " + os.Args[1])
	}

	if err = gcfg.ReadStringInto(&config, string(configBytes)); err != nil {
		return err
	}

	if len(config.Connection.RabbitmqURL) == 0 {
		return errors.New("RabbitMQ URL is empty or missing")
	}

	if len(config.Queue.Name) == 0 {
		return errors.New("Queue Name is empty or missing")
	}

	if config.Queue.WaitDelay < 30 || config.Queue.WaitDelay > 3600 {
		return errors.New("Queue Wait Delay must be between 30 and 3600 seconds")
	}

	if config.Message.DefaultTTL < 3600 || config.Message.DefaultTTL > 259200 {
		return errors.New("Message Default TTL must be between 3600 and 259200 seconds")
	}

	if config.Queue.PrefetchCount < 1 || config.Queue.PrefetchCount > 100 {
		return errors.New("PrefetchCount must be between 1 and 100")
	}

	if config.Connection.RetryDelay < 10 || config.Connection.RetryDelay > 300 {
		return errors.New("Connection Retry Delay must be between 10 and 300 seconds")
	}

	if config.Http.Timeout < 10 || config.Http.Timeout > 300 {
		return errors.New("Http Timeout must be between 10 and 300 seconds")
	}

	ok := false
	for ll := 0; ll < len(logLevels); ll++ {
		if logLevels[ll] == config.Log.Level {
			config.Log.LevelInt = ll
			ok = true
		}
	}
	if !ok {
		return errors.New("Invalid logging level specified")
	}

	return nil
}

/* Confirm queue configuration. Create queues if they don't exist. */
func runQueueCheck() {
	logPrintln(Info, "Checking RabbitMQ queues...")
	if err := queueCheck(); err != nil {
		logPrintln(Fatal, "Error detected while creating/confirming queue configuration:", err)
		os.Exit(1)
	}
	logPrintln(Info, "Queues are ready")
}

func queueCheck() error {
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

func consumeHttpRequests() {
	logPrintln(Info, "Connecting to RabbitMQ...")
	conn, err := amqp.Dial(config.Connection.RabbitmqURL)
	if err != nil {
		logPrintln(Warn, "Could not connect to RabbitMQ:", err)
		return
	}
	defer conn.Close()
	logPrintln(Info, "Connected successfully")

	logPrintln(Info, "Opening a channel to RabbitMQ...")
	ch, err := conn.Channel()
	if err != nil {
		logPrintln(Warn, "Could not open a channel:", err)
		return
	}
	defer ch.Close()
	logPrintln(Info, "Channel opened successfully")

	logPrintln(Info, "Setting prefetch count on the channel...")
	if err = ch.Qos(config.Queue.PrefetchCount, 0, false); err != nil {
		logPrintln(Warn, "Could not set prefetch count:", err)
		return
	}
	logPrintln(Info, "Set prefetch count successfully")

	logPrintln(Info, "Registering a consumer...")
	deliveries, err := ch.Consume(
		config.Queue.Name, // queue
		"",                // consumer
		false,             // auto-ack
		false,             // exclusive
		false,             // no-local
		false,             // no-wait
		nil,               // args
	)
	if err != nil {
		logPrintln(Warn, "Could not register a consumer:", err)
		return
	}
	logPrintln(Info, "Registered a consumer successfully")

	closedChannelListener := make(chan *amqp.Error, 1)
	ch.NotifyClose(closedChannelListener)
	logPrintln(Info, "Started 'closed channel' listener")

	var msg HttpRequestMessage

	// Go channel to coordinate acknowledgment of RabbitMQ messages
	ackCh := make(chan HttpRequestMessage, config.Queue.PrefetchCount)

	unacknowledgedMsgs := 0

	for {
		select {
		// Process next available message from RabbitMQ
		case delivery := <-deliveries:
			unacknowledgedMsgs++
			logPrintln(Debug, "Unacknowledged message count:", unacknowledgedMsgs)
			logPrintln(Info, "Message received from RabbitMQ. Parsing...")
			msg, err = parse(delivery)
			if err != nil {
				logPrintln(Warn, "Could not parse message:", err)
				msg.drop = true
				ackCh <- msg
			} else {
				logPrintln(Info, "Message parsed successfully")
				go msg.httpPost(ackCh)
			}

		// Acknowledge RabbitMQ messages and indicate whether they should be dropped or retried
		case msg = <-ackCh:
			logPrintln(Info, "Acknowledging message...")
			if err = msg.acknowledge(); err != nil {
				logPrintln(Warn, "Could not send message acknowledgement to RabbitMQ:", err)
				return
			}
			logPrintln(Info, "Message acknowledged successfully")

			unacknowledgedMsgs--
			logPrintln(Debug, "Unacknowledged message count:", unacknowledgedMsgs)
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
				logPrintln(Info, "Graceful restart requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				gracefulRestart = true
				if unacknowledgedMsgs == 0 {
					return
				}
			case "quit":
				logPrintln(Info, "Graceful shutdown requested")

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
		Url     string
		Headers []map[string]string
		Body    string
	}

	var fields MessageFields

	msg = HttpRequestMessage{delivery: rmqDelivery}

	/*** Parse fields in RabbitMQ message body ***/
	if err := json.Unmarshal(rmqDelivery.Body, &fields); err != nil {
		return msg, err
	}

	// Url
	if len(fields.Url) == 0 {
		err = errors.New("Field 'url' is empty or missing")
		return msg, err
	}
	msg.url = fields.Url

	// Http headers
	msg.headers = make(map[string]string)
	for _, m := range fields.Headers {
		for key, val := range m {
			msg.headers[key] = val
		}
	}

	// Request body
	msg.body = fields.Body

	/*** Extract fields from RabbitMQ message headers ***/
	rmqHeaders := rmqDelivery.Headers
	if rmqHeaders != nil {
		// Message expiration
		expirationStr, ok := rmqHeaders["expiration"]
		if ok {
			expiration, err := strconv.ParseInt(expirationStr.(string), 10, 64)
			if err != nil || (expiration != 0 && expiration < time.Now().Unix()) {
				err = errors.New("Header value 'expiration' is invalid, or the expiration time has already past.")
				return msg, err
			}
			msg.expiration = expiration
		}

		// Retry count and Time of first rejection. Will be empty if this is the first attempt.
		deathHistory, ok := rmqHeaders["x-death"]
		if ok {
			// The RabbitMQ "death" history is provided as an array of 2 maps.  One map has the history for the wait queue, the other for the main queue.
			// The "count" field will have the same value in each map and it represents the # of times this message was dead-lettered to each queue.
			// As an example, if the count is currently two, then there have been two previous attempts to send this message and the upcoming attempt will be the 2nd retry.
			queueDeathHistory := deathHistory.([]interface{})
			if len(queueDeathHistory) == 2 {
				mainQueueDeathHistory := queueDeathHistory[1].(amqp.Table)

				// Get retry count
				retryCount, retryCountOk := mainQueueDeathHistory["count"]
				if retryCountOk {
					msg.retry = int(retryCount.(int64))
				}

				// Get time of first rejection
				rejectTime, rejectTimeOk := mainQueueDeathHistory["time"]
				if rejectTimeOk {
					if rejectTime != nil {
						msg.firstRejectionTime = rejectTime.(time.Time).Unix()
					}
				}
			}
		}
	}

	/*** Extract fields from RabbitMQ message properties ***/
	// Message creation timestamp
	if (!rmqDelivery.Timestamp.IsZero()) {
		msg.timestamp = rmqDelivery.Timestamp.Unix()
	}

	// logPrintln(Debug, "Parsed fields:", fields)
	logPrintln(Debug, "Message fields:", msg)
	logPrintln(Debug, "Retry:", msg.retry)
	logPrintln(Debug, "First Rejection Time:", msg.firstRejectionTime)

	return msg, nil
}

func (msg HttpRequestMessage) httpPost(ackCh chan HttpRequestMessage) {
	req, err := http.NewRequest("POST", msg.url, bytes.NewBufferString(msg.body))
	if err != nil {
		logPrintln(Warn, "Invalid http request:", err)
		msg.drop = true
		ackCh <- msg
		return
	}

	client := &http.Client{Timeout: time.Duration(config.Http.Timeout) * time.Second}

	for hkey, hval := range msg.headers {
		req.Header.Set(hkey, hval)
	}

	logPrintln(Info, "Http POST Request url:", msg.url)
	resp, err := client.Do(req)

	if err != nil {
		logPrintln(Warn, "Error on http POST:", err)
		ackCh <- msg
		return
	} else {
		htmlData, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logPrintln(Info, "Error encountered when reading POST response body:", err)
		} else {
			logPrintln(Info, "POST response status code:", resp.StatusCode)
			logPrintln(Debug, "POST response body:", string(htmlData))
			resp.Body.Close()
		}
	}

	if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		logPrintln(Info, "4XX status on http POST (no retry):", resp.Status)
		msg.drop = true
		ackCh <- msg
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		logPrintln(Info, "Success on http POST:", resp.Status)
		msg.drop = true
		ackCh <- msg
		return
	}

	logPrintln(Warn, "Error on http POST:", resp.Status)
	ackCh <- msg
}

func (msg HttpRequestMessage) acknowledge() (err error) {
	if msg.drop {
		logPrintln(Info, "Sending ACK (drop) for request to url:", msg.url)
		return msg.delivery.Ack(false)
	}

	// Drop message if it will expire before the next retry
	expired := false
	if msg.expiration > 0 {
		// Use the expiration time included with the message, if one was provided
		expired = msg.expiration < (time.Now().Unix() + int64(config.Queue.WaitDelay))
	} else {
		// Otherwise, compare the default TTL to the time the message was first rejected
		if msg.firstRejectionTime > 0 {
			expired = (msg.firstRejectionTime + int64(config.Message.DefaultTTL)) < (time.Now().Unix() + int64(config.Queue.WaitDelay))
		}
	}

	if expired {
		logPrintln(Info, "Sending ACK (drop) for EXPIRED request to url:", msg.url)
		return msg.delivery.Ack(false)
	}

	logPrintln(Info, "Sending NACK (retry) for request to url:", msg.url)
	return msg.delivery.Nack(false, false)
}
