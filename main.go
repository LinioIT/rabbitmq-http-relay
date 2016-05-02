package main

import (
	"flag"
	"fmt"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/LinioIT/rabbitmq-worker/message"
	"github.com/LinioIT/rabbitmq-worker/rabbitmq"
	"github.com/streadway/amqp"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type Flags struct {
	DebugMode        bool
	QueuesOnly       bool
	gracefulShutdown bool
	gracefulRestart  bool
	connectionBroken bool
	cleanStart       bool
}

// Channel to receive asynchronous signals for graceful shutdown / restart
var signals chan os.Signal

func main() {
	var logFile logfile.Logger

	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGUSR1)

	// Parse command line arguments
	// func usage() provides help message for the command line
	flag.Usage = usage
	configFile, flags := getArgs()
	flags.cleanStart = true

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
			flags.connectionBroken = true
		} else {
			logFile.Write("Queues are ready")
		}

		if flags.QueuesOnly {
			logFile.Write("\"Queues Only\" option selected, exiting program.")
			break
		}

		// RabbitMQ queue verification must pass on the initial connection attempt
		if flags.cleanStart && flags.connectionBroken {
			logFile.Write("Initial RabbitMQ queue validation failed, exiting program.")
			break
		}

		// Process RabbitMQ messages
		if !flags.connectionBroken {
			consumeHttpRequests(&config, &flags, &logFile)
		}

		if checkShutdown(&flags, signals, &logFile, config.Connection.RetryDelay) {
			break
		}

		logFile.Close()
	}

	logFile.Close()
}

func getArgs() (configFile string, flags Flags) {
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

func consumeHttpRequests(config *config.ConfigParameters, flags *Flags, logFile *logfile.Logger) {
	var msg message.HttpRequestMessage
	var rmqConn rabbitmq.RMQConnection

	// Connect to RabbitMQ
	deliveries, closedChannelListener, err := rmqConn.Open(config, logFile)
	defer rmqConn.Close()
	if err != nil {
		logFile.Write(err)
		flags.connectionBroken = true
		return
	}

	// Create channel to coordinate acknowledgment of RabbitMQ messages
	ackCh := make(chan message.HttpRequestMessage, config.Queue.PrefetchCount)

	unacknowledgedMsgs := 0

	// Asynchronous event processing loop
	for {
		select {

		// Consume message from RabbitMQ
		case delivery := <-deliveries:
			unacknowledgedMsgs++
			logFile.WriteDebug("Unacknowledged message count:", unacknowledgedMsgs)
			logFile.WriteDebug("Message received from RabbitMQ. Parsing...")
			msg := message.HttpRequestMessage{}
			err = msg.Parse(delivery, logFile)
			if err != nil {
				logFile.Write("Could not parse Message ID", msg.MessageId, "-", err)
				msg.Drop = true
				ackCh <- msg
			} else {
				if msg.RetryCnt == 0 {
					logFile.Write("Message ID", msg.MessageId, "parsed successfully")
				} else {
					logFile.Write("Message ID", msg.MessageId, "parsed successfully - retry", msg.RetryCnt)
				}

				// Start goroutine to process http request
				go msg.HttpPost(ackCh, config.Http.Timeout)
			}

		// Log result of http request and acknowledge RabbitMQ message
		// The message will either be ACKed (dropped) or NACKed (retried)
		case msg = <-ackCh:
			if msg.HttpErr != nil {
				logFile.Write("Message ID", msg.MessageId, "http request error -", msg.HttpErr.Error())
			} else {
				if len(msg.HttpStatusMsg) > 0 {
					logFile.Write("Message ID", msg.MessageId, "http request success -", msg.HttpStatusMsg)
				} else {
					logFile.Write("Message ID", msg.MessageId, "http request was aborted or not attempted")
				}
			}

			if err = rabbitmq.Acknowledge(msg, config, logFile); err != nil {
				logFile.Write("RabbitMQ acknowledgment failed for Message ID", msg.MessageId, "-", err)
				return
			}
			logFile.WriteDebug("RabbitMQ acknowledgment successful for Message ID", msg.MessageId)

			unacknowledgedMsgs--
			logFile.WriteDebug("Unacknowledged message count:", unacknowledgedMsgs)

			if unacknowledgedMsgs == 0 && (flags.gracefulShutdown || flags.gracefulRestart) {
				return
			}

		// Was a problem detected with the RabbitMQ connection?
		// If yes, the main() loop will attempt to reconnect.
		case <-closedChannelListener:
			flags.connectionBroken = true
			return

		// Process os signals for graceful restart, graceful shutdown, or log reopen.
		case sig := <-signals:
			switch signalName := sig.String(); signalName {
			case "hangup":
				logFile.Write("Graceful restart requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				flags.gracefulRestart = true
				if unacknowledgedMsgs == 0 {
					return
				}

			case "quit":
				logFile.Write("Graceful shutdown requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = make(chan amqp.Delivery, 1)

				flags.gracefulShutdown = true
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

		if logFile.HasFatalError() && !flags.gracefulShutdown {
			fmt.Fprintln(os.Stderr, "Fatal log error detected. Starting graceful shutdown...")
			flags.gracefulShutdown = true
			if unacknowledgedMsgs == 0 {
				break
			}
		}
	}
}

func checkShutdown(flags *Flags, signals chan os.Signal, logFile *logfile.Logger, retryDelay int) bool {
	flags.cleanStart = false

	// Was a graceful shutdown requested?
	select {
	case sig := <-signals:
		if sig.String() == "quit" {
			logFile.Write("Shutdown request received, exiting program.")
			return true
		}
	default:
	}

	if flags.gracefulShutdown {
		if flags.connectionBroken {
			logFile.Write("Broken connection to RabbitMQ was detected during graceful shutdown, exiting program.")
		} else {
			logFile.Write("Graceful shutdown completed.")
		}
		return true
	}

	if flags.connectionBroken {
		flags.connectionBroken = false
		flags.gracefulRestart = false
		logFile.Write("Broken RabbitMQ connection detected. Reconnect will be attempted in", retryDelay, "seconds...")
		time.Sleep(time.Duration(retryDelay) * time.Second)
	}

	if flags.gracefulRestart {
		flags.gracefulRestart = false
		flags.cleanStart = true
		time.Sleep(2 * time.Second)
		logFile.Write("Restarting...")
	}

	return false
}
