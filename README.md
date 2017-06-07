RabbitMQ Worker for Reliable HTTP Dispatch
==========================================
RabbitMQ consumer, written in Go, which reliably sends requests to an http server.

**Description:**  
In order to reliably send an http request, a client inserts a message containing the request into a RabbitMQ queue. The Go consumer parses messages from this main queue and then attempts the request.  Failed requests are placed on a wait queue, where they stay until a configured delay period has expired. They are then moved back to the main queue for retry. Messages are retried until: the http request is successful, the message expiration time has been reached, or a permanent error has been encountered (e.g. message parsing error, 4XX http response code).

**Features include:**
- Concurrent execution of http requests in their own goroutines
- Monitoring of http response codes to determine message completion status:  
  2XX = Success, 4XX = No Retry. Any other response code or http error = Retry
- Message expiration can be specified per-message, or else defaults to the configured TTL
- Accepts OS signals to initiate admin operations:  
  QUIT = graceful shutdown, HUP = graceful restart, and USR1 = log reopen (for log rotation)
- Automatically detects if the RabbitMQ connection is broken and attempts to reconnect after a delay and graceful restart
- All message activity is logged
  - A command line 'debug' option can be used to show detailed internal activity
  - Logged messages include an id. The id is either provided by the client or generated from the message body and initial timestamp (if available)
  - Errors are also logged to a distinct error file for monitoring purposes

See the configuration file comments and the command-line description below for more details.


Installation
------------
These steps have been tested with Ubuntu 14.04, but should work for any Linux distro.

- Install RabbitMQ (version 3.6.1 or later): [RabbitMQ Install](http://www.rabbitmq.com/download.html)  
- Enable the following plugins using the 'rabbitmq-plugins' command: [RabbitMQ Plugins Manual Page](https://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html)  
  `rabbitmq_mqtt, mochiweb, webmachine, rabbitmq_web_dispatch, rabbitmq_management_agent, rabbitmq_management, rabbitmq_amqp1_0, amqp_client`
- Install and enable the RabbitMQ Message Timestamp plugin.  
  The Timestamp plugin can be downloaded here: [RabbitMQ Community Plugins](https://www.rabbitmq.com/community-plugins.html)  
  Instructions for installing additional plugins are here: [Additional Plugins](https://www.rabbitmq.com/installing-plugins.html)  
  *NOTE: The Timestamp plugin is not mandatory, but recommended since it is used to create a unique message id if one is not provided.*
- Add the RabbitMQ user, "rmq", and assign permissions:  
`sudo rabbitmqctl add_user rmq rmq`  
`sudo rabbitmqctl set_permissions rmq '.*' '.*' '.*'`  
`sudo rabbitmqctl set_user_tags rmq administrator`
- Download and install Go: [Downloads - The Go Programming Language](https://golang.org/dl/)  
  Follow instructions to:
  - Add the Go bin directory to the PATH environment variable
  - Setup a workspace
  - Set the GOPATH environment variable to point to the workspace.  
  - In addition, add the workspace bin directory, "$GOPATH/bin", to the PATH. This should appear in the shell startup script immediately after GOPATH is set.
- Install Git: [Setup Git](https://help.github.com/articles/set-up-git/)
- Download Go packages:  
  - go get gopkg.in/gcfg.v1
  - go get github.com/streadway/amqp
  - go get github.com/LinioIT/rabbitmq-worker
- Build and install executables:
  - go install github.com/LinioIT/rabbitmq-worker
  - go install github.com/LinioIT/rabbitmq-worker/insertHttpRequest
  - go install github.com/LinioIT/rabbitmq-worker/deleteQueue
  - go install github.com/LinioIT/rabbitmq-worker/webserver


Testing
-------
The included test suite exercises the worker. The log file, error file, and standard error outputs are verified against expected results.
A local http server is used to produce specific http responses and delays (see webserver/main.go and test/rw-test for details).
Currently, the tests take about 3 minutes to complete. Run the following commands to execute the tests:  
`cd $GOPATH/src/github.com/LinioIT/rabbitmq-worker/test`  
`./rw-test`

Sample successful test run:
```
Starting tests - Wed May 11 22:48:57 UTC 2016

Test 1 - Create Queues...                    PASSED
Test 2 - Startup...                          PASSED
Test 3 - Startup With Debug...               PASSED
Test 4 - Help...                             PASSED
Test 5 - Bad Option...                       PASSED
Test 6 - Missing Config File...              PASSED
Test 7 - Missing Queue Name...               PASSED
Test 8 - Invalid RabbitMQ URL...             PASSED
Test 9 - Graceful Restart...                 PASSED
Test 10 - Log Reopen...                      PASSED
Test 11 - 200 Response...                    PASSED
Test 12 - 404 Response...                    PASSED
Test 13 - 500 Response With Retries...       PASSED
Test 14 - Http Timeout...                    PASSED
Test 15 - Shutdown With Active Requests...   PASSED
Test 16 - Default TTL...                     PASSED
Test 17 - Prefetch Count...                  PASSED
Test 18 - Get Request...                     PASSED
Test 19 - Put Request...                     PASSED
Test 20 - Default Get Request...             PASSED
Test 21 - Wrong Method...                    PASSED

# of passed tests: 21
# of failed tests: 0

Tests completed - Wed May 11 22:51:47 UTC 2016
```


Usage
-----
```
Usage: rabbitmq-worker [OPTION] CONFIG_FILE

  --debug          Write debug-level messages to the log file
  -h, --help       Display this message
  --queues-only    Create/Verify RabbitMQ queues, then exit
```

**Queues**  
Two RabbitMQ queues are created when the worker begins, the main and wait queues. The name of the main queue is specified in
the config file and the wait queue is the same, with "_wait" appended. If the queues already exist, they are verified to
confirm their parameters. Failure to create or verify the queues will cause the worker to terminate.  
Please note that changes to the config file between executions of the worker, or upon a graceful restart, may cause
verification of the existing queues to fail. The offending queues should be deleted using RabbitMQ admin tools, so they can
be regenerated at the next invocation.  Here is a sample error reported when a queue cannot be verified:
```
2016-05-14T04:37:42Z - Error detected while creating/verifying queues: Could not declare queue notifier_wait
```

**Config File**  
Here is a sample config file with parameter descriptions:
```
[Connection]
RabbitmqURL = amqp://rmq:rmq@localhost:5672/
RetryDelay = 30        # Delay before attempting to restore a broken connection, in seconds

[Queue]
Name = notifier
WaitDelay = 30         # Message TTL for the wait queue, in seconds (delay between http request attempts)
PrefetchCount = 10     # Maximum number of simultaneous unacknowledged messages.
                       # This also controls the maximum number of http request threads.
                       # If set to 0, no maximum is enforced.  This is NOT recommended.

[Message]
DefaultTTL = 86400     # Default message TTL, in seconds.
                       # Total time a message can remain in RabbitMQ before being expired and dropped.
                       # This default is overridden if the "expiration" header of the RabbitMQ message
                       # contains a timestamp.

[Http]
DefaultMethod = POST   # Http method to be used if none is specified with the message.
                       # GET, POST, PUT, DELETE, etc...
Timeout = 30           # Http request timeout, in seconds

[Log]
LogFile = rabbitmq-worker.log
ErrFile = rabbitmq-worker.err
```

**Inserting Http Requests**  
Reliable delivery is initiated by inserting a message containing an http request into RabbitMQ.  
The http request is JSON encoded and inserted as the body of the RabboitMQ message. A message id and message expiration can
also be specified by inserting them into the headers of the RabbitMQ message.  The Go program insertHttpRequest/main.go has
been provided as an example of how to create a message.  

*JSON encoded http request:*  
```
| Field    | Required |
| -------- | -------- |
| url      |    Y     |
| method   |    N     |
| headers  |    N     |
| body     |    N     |

{"url": "http://localhost:8000/post/200/0", "method": "POST", "headers": [{"Content-Type": "application/json"}, {"Accept-Charset": "utf-8"}], "body": "{\"key\": \"1230789\"}"}
```

*RabbitMQ Message headers:*  
```
| Header      | Required | Description                                                          |
| ----------- | -------- | -------------------------------------------------------------------- |
| message_id  |    N     | Client-supplied id for tracking messages in the logs                 |
| expiration  |    N     | Timestamp to mark when the message will expire and should be dropped |

- If message_id is not provided, the worker will automatically generate an id.
  The format of the generated id is <<md5 of message body>>-<<original timestamp>> (if the timestamp plugin is enabled).
  e.g. 1ffcbf1f6044ae63631bd9d7c6967c51-1463192985

- If the expiration is not provided, then DefaultTTL from the config file is used
```

*insertHttpRequest:*  
Please review the source code (insertHttpRequest/main.go) for more information on inserting http request messages.
```
Usage: insertHttpRequest --url=URL [OPTION]

  --url            HTTP request URL. Required.
  --method         HTTP request method. If not provided, the default method will be used.
  --headers        HTTP request headers - JSON encoded array. Defaults to sample headers.
  --body           HTTP request body. Defaults to a sample JSON body. Double-quotes in JSON must be escaped.
  --id             Message ID. If not provided, an id will be generated.
  --ttl            Message TTL. If not provided, the default expiration will be used.

Example: insertHttpRequest --url=http://httpbin.org/post --method=POST --headers='[{"Content-Type": "application/json"}]' --body='{\"key\": \"1230789\"}' --id=MSGID00002 --ttl=55
```

**OS Signals**  
Operating system signals provide a means for performing admin tasks on a running worker:
```
| Signal   | Description                   |
| -------- | ----------------------------- |
| QUIT     | Graceful Shutdown             |
| HUP      | Graceful Restart              |
| USR1     | Log Reopen (for log rotation) |

e.g. kill -QUIT PROCESS_ID
```


Docker Container
----------------
**Build the container image**  
Install [Go](https://golang.org/dl/) and [Docker](https://www.docker.com/get-docker), then just:

    $ make

This will create a `linio/rabbitmq-worker` image.

**Run the container**  
To run the container and redirect the log files to stdout and stderr:
1) Set the stdout/stderr file paths in the configuration file

```
[Log]
LogFile = /dev/stdout
ErrFile = /dev/stderr
```

2) Mount the configuration file as a data volume and run the container

```
$ docker run -v $(pwd)/rabbitmq-worker.conf:/etc/rabbitmq-worker.conf linio/rabbitmq-worker
```

*Note: The log files can also be mounted as data volumes, instead of using stdout/stderr.*

**Shutdown and Restart**  
The container should be shutdown or restarted gracefully by passing an OS signal.

To shutdown the container:

    $ docker kill --signal=QUIT CONTAINER_ID

To restart the worker inside the container:

    $ docker kill --signal=HUP CONTAINER_ID


Future Enhancements
-------------------
- Exponential backoff, or some other technique to provide increasing delays on successive http retries
- Reporting to provide stats on message volumes and error rates
- Client callback for dropped, or excessively delayed, requests

