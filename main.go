package main

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/LinioIT/rabbitmq-worker/rabbitmq"
	"github.com/streadway/amqp"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Flags struct {
	DebugMode  bool
	QueuesOnly bool
}

// HttpRequestMessage holds all info and status for a RabbitMQ message and its associated http request.
type HttpRequestMessage struct {
	// RabbitMQ message
	delivery amqp.Delivery

	// Message Id is either set as a RabbitMQ message header, or
	// calculated as the md5 hash of the RabbitMQ message body
	messageId string

	// Http request fields
	url     string
	headers map[string]string
	body    string

	// Time when message was originally created (if timestamp plugin was installed)
	timestamp int64

	// Time when message will expire
	// (if not provided, value is calculated from DefaultTTL config setting)
	expiration int64

	// Retry history from RabbitMQ headers
	retryCnt           int
	firstRejectionTime int64

	// Http request status
	httpStatusMsg string
	httpRespBody  string
	httpErr       error

	// Drop / Retry Indicator - Set after http request attempt
	drop bool
}

var gracefulShutdown bool
var gracefulRestart bool
var connectionBroken bool

// Channel to receive asynchronous signals for graceful shutdown / restart
var signals chan os.Signal

func main() {
	var firstTime bool = true
	var logFile logfile.Logger

	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1)

	// Parse command line arguments
	// func usage() provides help message for the command line
	flag.Usage = usage
	configFile, flags := getArgs()

	config := config.ConfigParameters{}

	// Processing loop is re-executed anytime the RabbitMQ connection is broken, or a graceful restart is requested.
	for {
		if err := config.ParseConfigFile(configFile); err != nil {
			fmt.Fprintln(os.Stderr, "Could not load the configuration file:", configFile, "-", err)
			break
		}

		err := logFile.Open(config.Log.LogFile, flags.DebugMode)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not open the log file:", config.Log.LogFile, "-", err)
			break
		}

		logFile.Write("Configuration file loaded")
		logFile.WriteDebug("config:", config)

		logFile.Write("Creating/Verifying RabbitMQ queues...")
		if err := rabbitmq.QueueCheck(&config); err != nil {
			logFile.Write("Error detected while creating/verifying queues:", err)
			connectionBroken = true
		} else {
			logFile.Write("Queues are ready")
		}

		if flags.QueuesOnly {
			logFile.Write("\"Queues Only\" option selected, exiting program.")
			break
		}

		// RabbitMQ queue verification must pass on the initial connection attempt
		if firstTime && connectionBroken {
			logFile.Write("Initial RabbitMQ queue validation failed, exiting program.")
			break
		} else {
			firstTime = false
		}

		// Process RabbitMQ messages
		if !connectionBroken {
			consumeHttpRequests(&config, &logFile)
		} else {
			// Was a graceful shutdown requested?
			select {
			case sig := <-signals:
				if sig.String() == "quit" {
					logFile.Write("Shutdown request received, exiting program.")
					gracefulShutdown = true
				}
			default:
			}
			if gracefulShutdown {
				break
			}
		}

		if gracefulShutdown {
			if connectionBroken {
				logFile.Write("Broken connection to RabbitMQ was detected during graceful shutdown, exiting program.")
			} else {
				logFile.Write("Graceful shutdown completed.")
			}
			break
		}

		if connectionBroken {
			connectionBroken = false
			gracefulRestart = false
			logFile.Write("Broken RabbitMQ connection detected. Reconnect will be attempted in", config.Connection.RetryDelay, "seconds...")
			time.Sleep(time.Duration(config.Connection.RetryDelay) * time.Second)
		}

		if gracefulRestart {
			gracefulRestart = false
			time.Sleep(2 * time.Second)
			logFile.Write("Restarting...")
		}

		logFile.Close()
	}

	logFile.Close()
}

func getArgs() (configFile string, flags Flags) {
	flags.DebugMode = false
	flags.QueuesOnly = false

	flag.BoolVar(&flags.DebugMode, "debug", false, "Enable debug messages - Bool")
	flag.BoolVar(&flags.QueuesOnly, "queues-only", false, "Create/Verify queues only - Bool")

	flag.Parse()

	argCnt := len(flag.Args())
	if argCnt == 1 {
		configFile = flag.Args()[0]
	} else {
		usage()
	}

	return
}

func usage() {
	fmt.Fprintln(os.Stderr, "Usage:", os.Args[0], "[OPTION] CONFIG_FILE\n")
	fmt.Fprintln(os.Stderr, "  --debug          Write debug-level messages to the log file")
	fmt.Fprintln(os.Stderr, "  -h, --help       Display this message")
	fmt.Fprintln(os.Stderr, "  --queues-only    Create/Verify RabbitMQ queues, then exit")
	fmt.Fprintln(os.Stderr, " ")
	os.Exit(1)
}

func consumeHttpRequests(config *config.ConfigParameters, logFile *logfile.Logger) {
	var msg HttpRequestMessage
	var rmqConn rabbitmq.RMQConnection

	deliveries, closedChannelListener, err := rmqConn.Open(config, logFile)
	defer rmqConn.Close()
	if err != nil {
		logFile.Write(err)
		connectionBroken = true
		return
	}

	// Create channel to coordinate acknowledgment of RabbitMQ messages
	ackCh := make(chan HttpRequestMessage, config.Queue.PrefetchCount)

	unacknowledgedMsgs := 0

	// Asynchronous event processing loop
	for {
		select {

		// Process next available message from RabbitMQ
		case delivery := <-deliveries:
			unacknowledgedMsgs++
			logFile.WriteDebug("Unacknowledged message count:", unacknowledgedMsgs)
			logFile.WriteDebug("Message received from RabbitMQ. Parsing...")
			msg, err = parse(delivery, logFile)
			if err != nil {
				logFile.Write("Could not parse Message ID", msg.messageId, "-", err)
				msg.drop = true
				ackCh <- msg
			} else {
				if msg.retryCnt == 0 {
					logFile.Write("Message ID", msg.messageId, "parsed successfully")
				} else {
					logFile.Write("Message ID", msg.messageId, "parsed successfully - retry", msg.retryCnt)
				}

				// Start goroutine to process http request
				go msg.httpPost(ackCh, config.Http.Timeout)
			}

		// Log result of http request and ACK (drop) or NACK (retry) RabbitMQ Message
		case msg = <-ackCh:
			if msg.httpErr != nil {
				logFile.Write("Message ID", msg.messageId, "http request error -", msg.httpErr.Error())
			} else {
				if len(msg.httpStatusMsg) > 0 {
					logFile.Write("Message ID", msg.messageId, "http request success -", msg.httpStatusMsg)
				} else {
					logFile.Write("Message ID", msg.messageId, "http request was aborted or not attempted")
				}
			}

			if err = msg.acknowledge(config, logFile); err != nil {
				logFile.Write("RabbitMQ acknowledgment failed for Message ID", msg.messageId, "-", err)
				return
			}
			logFile.WriteDebug("RabbitMQ acknowledgment successful for Message ID", msg.messageId)

			unacknowledgedMsgs--
			logFile.WriteDebug("Unacknowledged message count:", unacknowledgedMsgs)

			if unacknowledgedMsgs == 0 && (gracefulShutdown || gracefulRestart) {
				return
			}

		// Was a problem detected with the RabbitMQ connection?
		// If yes, the main() loop will attempt to reconnect.
		case <-closedChannelListener:
			connectionBroken = true
			return

		// Process os signals for graceful shutdown, graceful restart, or log reopen.
		case sig := <-signals:
			switch signalName := sig.String(); signalName {
			case "hangup":
				logFile.Write("Graceful restart requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				gracefulRestart = true
				if unacknowledgedMsgs == 0 {
					return
				}
			case "quit":
				logFile.Write("Graceful shutdown requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				gracefulShutdown = true
				if unacknowledgedMsgs == 0 {
					return
				}

			case "user defined signal 1":
				logFile.Write("Log reopen requested")
				if err := logFile.Reopen(); err != nil {
					logFile.Write("Error encountered during log reopen -", err)
				} else {
					logFile.Write("Log reopen completed")
				}
			}
		}

		if logFile.HasFatalError() && !gracefulShutdown {
			fmt.Fprintln(os.Stderr, "Fatal log error detected. Starting graceful shutdown...")
			gracefulShutdown = true
			if unacknowledgedMsgs == 0 {
				break
			}
		}
	}
}

func parse(rmqDelivery amqp.Delivery, logFile *logfile.Logger) (msg HttpRequestMessage, err error) {
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

	/*** Extract fields from RabbitMQ message properties ***/
	// Message creation timestamp
	if !rmqDelivery.Timestamp.IsZero() {
		msg.timestamp = rmqDelivery.Timestamp.Unix()
	}

	/*** Extract fields from RabbitMQ message headers ***/
	rmqHeaders := rmqDelivery.Headers
	if rmqHeaders != nil {

		// Message expiration
		expirationHdr, ok := rmqHeaders["expiration"]
		if ok {
			expiration, ok := expirationHdr.(int64)
			if !ok || expiration < time.Now().Unix() {
				logFile.Write("Header value 'expiration' is invalid, or the expiration time has already past. Default TTL will be used.")
			} else {
				msg.expiration = expiration
			}
		}

		// Message ID
		messageIdHdr, ok := rmqHeaders["message_id"]
		if ok {
			messageId, ok := messageIdHdr.(string)
			if !ok || len(messageId) == 0 {
				logFile.Write("Header value 'message_id' is invalid or empty. The Message ID will be the md5 hash of the RabbitMQ message body.")
			} else {
				msg.messageId = messageId
			}
		}
		// Message ID was not provided, set it as the md5 hash of the message body. Append the original timestamp, if available.
		if len(msg.messageId) == 0 {
			msg.messageId = fmt.Sprintf("%x", md5.Sum(rmqDelivery.Body))
			if msg.timestamp > 0 {
				msg.messageId += fmt.Sprintf("-%d", msg.timestamp)
			}
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
					msg.retryCnt = int(retryCount.(int64))
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

	logFile.WriteDebug("Message fields:", msg)
	logFile.WriteDebug("Retry:", msg.retryCnt)
	logFile.WriteDebug("First Rejection Time:", msg.firstRejectionTime)

	return msg, nil
}

func (msg HttpRequestMessage) httpPost(ackCh chan HttpRequestMessage, timeout int) {
	req, err := http.NewRequest("POST", msg.url, bytes.NewBufferString(msg.body))
	if err != nil {
		msg.httpErr = err
		msg.httpStatusMsg = "Invalid http request: " + err.Error()
		msg.drop = true
		ackCh <- msg
		return
	}

	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}

	for hkey, hval := range msg.headers {
		req.Header.Set(hkey, hval)
	}

	resp, err := client.Do(req)

	if err != nil {
		msg.httpErr = err
		msg.httpStatusMsg = "Error on http POST: " + err.Error()
		ackCh <- msg
		return
	} else {
		htmlData, err := ioutil.ReadAll(resp.Body)

		// The response body is not currently used to evaluate success of the http request. Therefore, an error here is not fatal.
		// This will change if functionality is added to evaluate the response body.
		if err != nil {
			msg.httpRespBody = "Error encountered when reading POST response body"
		} else {
			msg.httpStatusMsg = resp.Status
			msg.httpRespBody = string(htmlData)
			resp.Body.Close()
		}
	}

	if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		msg.httpErr = errors.New("4XX status on http POST (no retry): " + resp.Status)
		msg.drop = true
		ackCh <- msg
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		msg.drop = true
		ackCh <- msg
		return
	}

	msg.httpErr = errors.New("Error on http POST: " + resp.Status)
	ackCh <- msg
}

func (msg HttpRequestMessage) acknowledge(config *config.ConfigParameters, logFile *logfile.Logger) (err error) {
	if msg.drop {
		logFile.Write("Dropping Message ID", msg.messageId)
		logFile.WriteDebug("Sending ACK (drop) for Message ID", msg.messageId)
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
		logFile.Write("Message ID", msg.messageId, "has EXPIRED")
		logFile.WriteDebug("Sending ACK (drop) for EXPIRED Message ID", msg.messageId)
		return msg.delivery.Ack(false)
	}

	logFile.Write("Message ID", msg.messageId, "will be retried")
	logFile.WriteDebug("Sending NACK (retry) for Message ID", msg.messageId)
	return msg.delivery.Nack(false, false)
}
