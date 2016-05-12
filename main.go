package main

import (
	"flag"
	"fmt"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/LinioIT/rabbitmq-worker/message"
	"github.com/LinioIT/rabbitmq-worker/rabbitmq"
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

// Channel to receive asynchronous signals:
//   QUIT - graceful shutdown
//   HUP - graceful restart
//   USR1 - log reopen
var signals chan os.Signal

func main() {
	var logFile logfile.Logger
	var errFile logfile.Logger

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

		err = errFile.Open(config.Log.ErrFile, false)
		if err != nil {
			fmt.Fprintln(os.Stderr, "Could not open the error file:", config.Log.ErrFile, "-", err)
			break
		}

		logFile.Write("Configuration file loaded")
		logFile.WriteDebug("Config settings:\n" + config.String())

		logFile.Write("Creating/Verifying RabbitMQ queues...")
		if err := rabbitmq.QueueCheck(&config); err != nil {
			logFile.Write("Error detected while creating/verifying queues:", err)
			errFile.Write("Error detected while creating/verifying queues:", err)
			flags.connectionBroken = true
		} else {
			logFile.Write("Queues are ready")
		}

		if flags.QueuesOnly {
			logFile.Write("\"Queues Only\" option selected, exiting program.")
			break
		}

		// Queue verification must pass on the first attempt after starting or after a graceful restart.
		if flags.cleanStart && flags.connectionBroken {
			logFile.Write("Initial RabbitMQ queue verification failed, exiting program.")
			errFile.Write("Initial RabbitMQ queue verification failed, exiting program.")
			break
		}

		// Process RabbitMQ messages
		if !flags.connectionBroken {
			consumeHttpRequests(&config, &flags, &logFile, &errFile)
		}

		if checkShutdown(&flags, signals, &logFile, config.Connection.RetryDelay) {
			break
		}

		logFile.Close()
		errFile.Close()
	}

	logFile.Close()
	errFile.Close()
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

func consumeHttpRequests(config *config.ConfigParameters, flags *Flags, logFile *logfile.Logger, errFile *logfile.Logger) {
	var msg message.HttpRequestMessage
	var rmqConn rabbitmq.RMQConnection

	// Connect to RabbitMQ
	deliveries, closedChannelListener, err := rmqConn.Open(config, logFile)
	defer rmqConn.Close()
	if err != nil {
		logFile.Write(err)
		errFile.Write(err)
		flags.connectionBroken = true
		return
	}

	// Create channel to acknowledge processed RabbitMQ messages
	ackCh := make(chan message.HttpRequestMessage, config.Queue.PrefetchCount)

	unacknowledgedMsgs := 0

	// Asynchronous event processing loop
	for {
		select {

		// Consume message from RabbitMQ
		case delivery := <-deliveries:
			unacknowledgedMsgs++
			logFile.WriteDebug("Message received from RabbitMQ. Unacknowledged message count:", unacknowledgedMsgs)
			msg := message.HttpRequestMessage{}
			err = msg.Parse(delivery, logFile)
			if err != nil {
				logFile.Write("Message ID", msg.MessageId, "- Parse failed:", err)
				errFile.Write("Message ID", msg.MessageId, "- Parse failed:", err)
				msg.Drop = true
				ackCh <- msg
			} else {
				if msg.RetryCnt == 0 {
					logFile.Write("Message ID", msg.MessageId, "- Parsed successfully")
				} else {
					logFile.Write("Message ID", msg.MessageId, "- Parsed successfully, Retry Count =", msg.RetryCnt)
				}

				// Start goroutine to process http request
				go msg.HttpRequest(ackCh, config.Http.DefaultMethod, config.Http.Timeout)
			}

		// Log result of http request and acknowledge RabbitMQ message
		// The message will either be ACKed (dropped) or NACKed (retried)
		case msg = <-ackCh:
			if msg.HttpErr != nil {
				logFile.Write("Message ID", msg.MessageId, "- Http request error:", msg.HttpErr.Error())
				errFile.Write("Message ID", msg.MessageId, "- Http request error:", msg.HttpErr.Error())
			} else {
				if len(msg.HttpStatusMsg) > 0 {
					logFile.Write("Message ID", msg.MessageId, "- Http request success:", msg.HttpStatusMsg)
					logFile.WriteDebug("Message ID", msg.MessageId, "- Response Body:", msg.HttpRespBody)
				} else {
					logFile.Write("Message ID", msg.MessageId, "- Http request was aborted or not attempted")
					errFile.Write("Message ID", msg.MessageId, "- Http request was aborted or not attempted")
				}
			}

			msg.CheckExpiration(config.Queue.WaitDelay, config.Message.DefaultTTL)
			if msg.Expired {
				errFile.Write("Message ID", msg.MessageId, "- EXPIRED")
			}

			// Acknowledge RabbitMQ message to trigger drop or retry
			if err = rabbitmq.Acknowledge(msg, logFile); err != nil {
				logFile.Write("Message ID", msg.MessageId, "- RabbitMQ acknowledgment failed:", err)
				errFile.Write("Message ID", msg.MessageId, "- RabbitMQ acknowledgment failed:", err)
				return
			}
			logFile.WriteDebug("Message ID", msg.MessageId, "- RabbitMQ acknowledgment successful")

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
				deliveries = rabbitmq.GetDeliveryChan(1)

				flags.gracefulRestart = true
				if unacknowledgedMsgs == 0 {
					return
				}

			case "quit":
				logFile.Write("Graceful shutdown requested")

				// Substitute a dummy delivery channel to halt consumption from RabbitMQ
				deliveries = rabbitmq.GetDeliveryChan(1)

				flags.gracefulShutdown = true
				if unacknowledgedMsgs == 0 {
					return
				}

			case "user defined signal 1":
				logFile.Write("Reopen of log files requested")
				if err := logFile.Reopen(); err != nil {
					errFile.Write("Error encountered during log file reopen -", err)
				} else {
					logFile.Write("Log file reopen completed")
				}
				if err := errFile.Reopen(); err != nil {
					fmt.Fprintln(os.Stderr, "Error encountered during error file reopen -", err)
				} else {
					logFile.Write("Error file reopen completed")
				}
			}
		}

		// Shutdown if an error has been detected when writing to the log files
		if !flags.gracefulShutdown {
			if logFile.HasFatalError() {
				fmt.Fprintln(os.Stderr, "Fatal problem with log file detected. Starting graceful shutdown...")
				flags.gracefulShutdown = true
				if unacknowledgedMsgs == 0 {
					break
				}
			} else if errFile.HasFatalError() {
				fmt.Fprintln(os.Stderr, "Fatal problem with error file detected. Starting graceful shutdown...")
				flags.gracefulShutdown = true
				if unacknowledgedMsgs == 0 {
					break
				}
			}
		}
	}
}

func checkShutdown(flags *Flags, signals chan os.Signal, logFile *logfile.Logger, retryDelay int) (quit bool) {
	quit = true
	flags.cleanStart = false

	// Was a graceful shutdown requested?
	select {
	case sig := <-signals:
		if sig.String() == "quit" {
			logFile.Write("Shutdown request received, exiting program.")
			return
		}
	default:
	}

	if flags.gracefulShutdown {
		if flags.connectionBroken {
			logFile.Write("Broken connection to RabbitMQ was detected during graceful shutdown, exiting program.")
		} else {
			logFile.Write("Graceful shutdown completed.")
		}
		return
	}

	quit = false

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

	return
}
