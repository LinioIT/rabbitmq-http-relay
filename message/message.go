package message

import (
	"bytes"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/logfile"
	"github.com/streadway/amqp"
	"io/ioutil"
	"net/http"
	"time"
)

// HttpRequestMessage holds all info and status for a RabbitMQ message and its associated http request.
type HttpRequestMessage struct {
	// RabbitMQ message
	Delivery amqp.Delivery

	// Message Id is retrieved from the RabbitMQ header message_id.
	// If not provided in the header, it is generated by concatenating the md5 hash of
	// the message body with the original RabbitMQ timestamp, if the timestamp is available.
	// The generated format will be either md5 or md5-timestamp.
	// e.g. "ee6ad4b95b3970ed69b847db88757922-1462230252"
	MessageId string

	// Http request fields
	Method  string
	Url     string
	Headers map[string]string
	Body    string

	// Time when message was originally created (if timestamp plugin was installed)
	Timestamp int64

	// Time when message will expire, as provided in RabbitMQ message header.
	// If not provided, this value is 0 and the expiration is calculated from the DefaultTTL setting.
	Expiration int64

	// Retry history from RabbitMQ headers
	RetryCnt           int
	FirstRejectionTime int64

	// Http request status
	HttpStatusMsg string
	HttpRespBody  string
	HttpErr       error

	// Message disposition
	Drop    bool // Drop/Retry indicator
	Expired bool // Message expired
}

func (msg *HttpRequestMessage) Parse(rmqDelivery amqp.Delivery, logFile *logfile.Logger) (err error) {
	type MessageFields struct {
		Method  string
		Url     string
		Headers []map[string]string
		Body    string
	}
	var fields MessageFields

	msg.Delivery = rmqDelivery

	// Get message creation timestamp, if any
	if !rmqDelivery.Timestamp.IsZero() {
		msg.Timestamp = rmqDelivery.Timestamp.Unix()
	}

	// Generate a default Message ID, in case one was not provided.
	// Set it as the md5 hash of the RabbitMQ message body, appended with the original timestamp if available.
	msg.MessageId = fmt.Sprintf("%x", md5.Sum(rmqDelivery.Body))
	if msg.Timestamp > 0 {
		msg.MessageId += fmt.Sprintf("-%d", msg.Timestamp)
	}

	var ok bool

	/*** Extract fields from RabbitMQ message headers ***/
	rmqHeaders := rmqDelivery.Headers
	if rmqHeaders != nil {

		// Message ID
		messageIdHdr, ok := rmqHeaders["message_id"]
		if ok {
			messageId, ok := messageIdHdr.(string)
			if ok && len(messageId) > 0 {
				msg.MessageId = messageId
			}
		}

		// Message expiration
		expirationHdr, ok := rmqHeaders["expiration"]
		if ok {
			expiration, ok := expirationHdr.(int64)
			if !ok || expiration < time.Now().Unix() {
				return errors.New("Header value 'expiration' is invalid, or the expiration time has already past.")
			} else {
				msg.Expiration = expiration
			}
		}

		// Get retry count and time of first rejection. Both values will be zero if this is the first attempt.
		msg.RetryCnt, msg.FirstRejectionTime = getRetryInfo(rmqHeaders)
	}

	/*** Parse fields in RabbitMQ message body ***/
	if err := json.Unmarshal(rmqDelivery.Body, &fields); err != nil {
		return err
	}

	// Method
	if len(fields.Method) > 0 {
		fields.Method, ok = config.CheckMethod(fields.Method)
		if !ok {
			return errors.New("The value in field 'method' is not a recognized http method")
		}
		msg.Method = fields.Method
	}

	// Url
	if len(fields.Url) == 0 {
		return errors.New("Field 'url' is empty or missing")
	}
	msg.Url = fields.Url

	// Http headers
	msg.Headers = make(map[string]string)
	for _, m := range fields.Headers {
		for key, val := range m {
			msg.Headers[key] = val
		}
	}

	// Request body
	msg.Body = fields.Body

	if len(msg.Method) > 0 {
		logFile.WriteDebug("Message ID", msg.MessageId, "- Method:", msg.Method)
	}
	logFile.WriteDebug("Message ID", msg.MessageId, "- Url:", msg.Url)
	logFile.WriteDebug("Message ID", msg.MessageId, "- Headers:", msg.Headers)
	logFile.WriteDebug("Message ID", msg.MessageId, "- Body:", msg.Body)
	logFile.WriteDebug("Message ID", msg.MessageId, "- Expiration:", msg.Expiration)

	if msg.RetryCnt > 0 {
		logFile.WriteDebug("Message ID", msg.MessageId, "- Retry Count:", msg.RetryCnt)
		logFile.WriteDebug("Message ID", msg.MessageId, "- First Rejection Time:", msg.FirstRejectionTime)
	}

	return nil
}

func getRetryInfo(rmqHeaders amqp.Table) (retryCnt int, firstRejectionTime int64) {
	retryCnt = 0
	firstRejectionTime = 0

	deathHistory, ok := rmqHeaders["x-death"]
	if ok {
		// The RabbitMQ "death" history is provided as an array of 2 maps.  One map has the history for the wait queue, the other for the main queue.
		// The "count" field will have the same value in each map and it represents the # of times this message was dead-lettered to each queue.
		// As an example, if the count is currently two, then there have been two previous attempts to send this message and the upcoming attempt will be the 2nd retry.
		queueDeathHistory := deathHistory.([]interface{})
		if len(queueDeathHistory) == 2 {
			mainQueueDeathHistory := queueDeathHistory[1].(amqp.Table)

			// Get retry count
			retries, retriesOk := mainQueueDeathHistory["count"]
			if retriesOk {
				retryCnt = int(retries.(int64))
			}

			// Get time of first rejection
			rejectTime, rejectTimeOk := mainQueueDeathHistory["time"]
			if rejectTimeOk {
				if rejectTime != nil {
					firstRejectionTime = rejectTime.(time.Time).Unix()
				}
			}
		}
	}

	return
}

func (msg *HttpRequestMessage) HttpRequest(ackCh chan HttpRequestMessage, defaultMethod string, timeout int) {
	var method string
	if len(msg.Method) > 0 {
		method = msg.Method
	} else {
		method = defaultMethod
	}

	req, err := http.NewRequest(method, msg.Url, bytes.NewBufferString(msg.Body))
	if err != nil {
		msg.HttpErr = err
		msg.HttpStatusMsg = "Invalid http request: " + err.Error()
		msg.Drop = true
		ackCh <- *msg
		return
	}

	client := &http.Client{Timeout: time.Duration(timeout) * time.Second}

	for hkey, hval := range msg.Headers {
		req.Header.Set(hkey, hval)
	}

	resp, err := client.Do(req)

	if err != nil {
		msg.HttpErr = err
		msg.HttpStatusMsg = "Error on http request: " + err.Error()
		ackCh <- *msg
		return
	} else {
		htmlData, err := ioutil.ReadAll(resp.Body)

		// The response body is not currently used to evaluate success of the http request. Therefore, an error here is not fatal.
		// This will change if functionality is added to evaluate the response body.
		if err != nil {
			msg.HttpRespBody = "Error encountered when reading response body"
		} else {
			msg.HttpStatusMsg = resp.Status
			msg.HttpRespBody = string(htmlData)
			resp.Body.Close()
		}
	}

	if resp.StatusCode >= 400 && resp.StatusCode <= 499 {
		msg.HttpErr = errors.New("4XX status on http request (no retry): " + resp.Status)
		msg.Drop = true
		ackCh <- *msg
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		msg.Drop = true
		ackCh <- *msg
		return
	}

	msg.HttpErr = errors.New("Error on http request: " + resp.Status)
	ackCh <- *msg
}

func (msg *HttpRequestMessage) CheckExpiration(waitDelay, defaultTTL int) {
	// Use the expiration time included with the message, if one was provided
	if msg.Expiration > 0 {
		msg.Expired = msg.Expiration < (time.Now().Unix() + int64(waitDelay))
	} else {
		// Otherwise, compare the default TTL to the time the message was first rejected
		if msg.FirstRejectionTime > 0 {
			msg.Expired = (msg.FirstRejectionTime + int64(defaultTTL)) < (time.Now().Unix() + int64(waitDelay))
		}
	}

	return
}
