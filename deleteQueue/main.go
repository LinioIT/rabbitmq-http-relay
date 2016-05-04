package main

import (
	"flag"
	"fmt"
	"github.com/LinioIT/rabbitmq-worker/config"
	"github.com/LinioIT/rabbitmq-worker/rabbitmq"
	"os"
)

func main() {
	flag.Usage = usage
	configFile := getArgs()

	config := config.ConfigParameters{}

	if err := config.ParseConfigFile(configFile); err != nil {
		fmt.Fprintln(os.Stderr, "Could not load the configuration file:", configFile, "-", err)
		os.Exit(1)
	}

	if err := rabbitmq.QueueDelete(&config); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	} else {
		fmt.Println("Queues", config.Queue.Name, "and", config.Queue.Name+"_wait", "deleted successfully.\n")
	}
}

func getArgs() (configFile string) {
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
	fmt.Fprintln(os.Stderr, "Usage:", os.Args[0], "CONFIG_FILE\n")
	os.Exit(1)
}
