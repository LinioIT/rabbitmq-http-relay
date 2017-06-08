#!/bin/bash

if    [[ $# -ne 1 ]]
then  echo
      echo usage: docker_signal.sh SIGNAL
      echo
      exit 1
fi

if    [[ $1 != "HUP"  &&  $1 != "QUIT" ]]
then  echo
      echo Unrecognized signal: $1
      echo
      exit 1
fi

CONTAINER=$(docker ps --filter ancestor=rabbitmq-worker | tail -1 | sed 's/^.*\s//')

if    [[ $CONTAINER == NAMES ]]
then  echo
      echo Could not find a running rabbitmq-worker container!
      echo
      exit 1
fi

docker kill --signal=$1 $CONTAINER
