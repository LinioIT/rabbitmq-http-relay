#!/bin/bash

###  SET HOST FILE PATHS HERE. All paths must be absolute!  ###
HOST_CFG=$(pwd)/rabbitmq-worker.conf
HOST_LOG=$(pwd)/rabbitmq-worker.log
HOST_ERR=$(pwd)/rabbitmq-worker.err
###############################################################

## Automated setting of file paths to be used in the container ##
# Note: Log and error file paths come from the host config file. All paths must be absolute!
DOCKER_CFG=/etc/rabbitmq-worker.conf
# Get log and error file paths from the config file
DOCKER_LOG=$(grep "^LogFile" $HOST_CFG | sed 's/^LogFile\s*=\s*//')
DOCKER_ERR=$(grep "^ErrFile" $HOST_CFG | sed 's/^ErrFile\s*=\s*//')

## Confirm that host files exist ##
touch $HOST_LOG $HOST_ERR 2> /dev/null
if    [[ ! -f $HOST_LOG ]]
then  echo -e Log file $HOST_LOG could not be created "\n"
      exit 1
fi
if    [[ ! -f $HOST_ERR ]]
then  echo -e Error file $HOST_ERR could not be created "\n"
      exit 1
fi
if    [[ ! -s $HOST_CFG ]]
then  echo -e Config file $HOST_CFG is either empty or does not exist"\n"
      exit 1
fi

## Mount data volumes and run rabbitmq-worker in a docker container ##
docker run -d -v $HOST_CFG:$DOCKER_CFG -v $HOST_LOG:$DOCKER_LOG -v $HOST_ERR:$DOCKER_ERR rabbitmq-worker

## List running rabbitmq-worker container ##
docker ps --filter ancestor=rabbitmq-worker
