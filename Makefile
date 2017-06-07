CHECK = \033[32m✔\033[39m
PROJECT_NAME=rabbitmq-worker
IMAGE_NAME=tsilvers/$(PROJECT_NAME)
GOPATH=$(shell pwd)/vendor:$(shell pwd)
GOBIN=$(shell pwd)/bin

build: get binary docker

get:
	@echo -n "Getting dependencies...                          "
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) go get .
	@echo "$(CHECK) Done"

binary:
	@echo -n "Creating binary...                               "
	@GOPATH=$(GOPATH) GOBIN=$(GOBIN) CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o bin/$(PROJECT_NAME) .
	@echo "$(CHECK) Done"

docker:
	docker build -t $(PROJECT_NAME) .
	@echo "Docker image creation...                         $(CHECK) Done"

.PHONY: get binary docker
