RabbitMQ Worker for Reliable HTTP Dispatch
==========================================
RabbitMQ consumer, written in Go, which reliably sends requests to an http server.

Features include:
- A "wait" queue for holding and retrying failed requests
- Monitoring of http response codes to determine message completion status:  
  2XX = success, 4XX = no retry. Any other response code or http error = retry
- Message expiration can be specified per-message, or else defaults to the configuration TTL
- All message activity is logged. A command line 'debug' option can be used to get detailed internal activity
- Accepts os signals to initiate admin operations: graceful shutdown, graceful restart, and log reopen (for log rotation)
- Http requests are executed in their own goroutines to enhance performance
- Automatically detects when the RabbitMQ connection is broken and reconnects (after a graceful restart)

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


Testing
-------
The included test suite exercises the worker. The log and standard error outputs are verified against the expected results.
An external website is used to produce specific http responses and delays (see test script rw-test for details).
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
