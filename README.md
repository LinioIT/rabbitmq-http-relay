RabbitMQ Worker for HTTP Dispatch with Delayed Retry
====================================================
Ubuntu Vagrant Box with [Go](http://golang.org) + [RabbitMQ](http://www.rabbitmq.com) + [Go AMQP Client](https://github.com/streadway/amqp).


Configured Versions
-------------------
- Ubuntu: 14.04
- Go: 1.6.1


Requirements for Installation
-----------------------------
- VirtualBox
- Vagrant


Installation
------------
- vagrant up --provision  
*Current directory on host will be shared with /vagrant in vagrant box*


Testing
-------
- vagrant ssh
- go run /vagrant/send.go  
Sample output:
```
2016/03/16 21:31:19  [x] Sent hello
```
- go run /vagrant/receive.go  
Sample output:
```
2016/03/16 21:31:23  [*] Waiting for messages. To exit press CTRL+C
2016/03/16 21:31:23 Received a message: hello
```
- *Press Ctrl-C to exit receive.go*


Ansible Role Variables for Go Installation
------------------------------------------
All of these variables are optional and should only be changed if you need to install a different version of Go (e.g. if you are installing on FreeBSD, or if you need to use an earlier release).

- go_tarball: The tarball that you want to install. A list of options can be found on the [Go Downloads page](http://code.google.com/p/go/downloads/list). The default is the official x86 64-bit Linux tarball for the latest stable release.

- go_tarball_checksum: The SHA256 checksum for the tarball that you want to install. This can be calculated using `sha256sum` if necessary. The default is the SHA256 checksum of the official x86 64-bit tarball for the latest stable release.

- go_version_target: The string that the `go version` command is expected to return (e.g. "go version go1.2.1 linux/amd64"). This variable is used to control whether or not the Go tarball should be extracted, thereby upgrading (or downgrading) a previously installed copy. If the installed version already matches the target, the extraction step is skipped.

- go_download_location: The full download URL. This variable simply appends the go_tarball variable onto the Go Download URL. This should not need to be modified.

