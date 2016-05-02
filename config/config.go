package config

import (
	"errors"
	"gopkg.in/gcfg.v1"
	"io/ioutil"
	"strconv"
)

type ConfigParameters struct {
	Connection struct {
		RabbitmqURL string
		RetryDelay  int
	}
	Queue struct {
		Name          string
		WaitDelay     int
		PrefetchCount int
	}
	Message struct {
		DefaultTTL int
	}
	Http struct {
		Timeout int
	}
	Log struct {
		LogFile string
	}
}

func (config *ConfigParameters) ParseConfigFile(configFile string) error {
	configBytes, err := ioutil.ReadFile(configFile)
	if err != nil {
		return errors.New("Error encountered reading file " + configFile)
	}

	if err = gcfg.ReadStringInto(config, string(configBytes)); err != nil {
		return err
	}

	if len(config.Connection.RabbitmqURL) == 0 {
		return errors.New("RabbitMQ URL is empty or missing")
	}

	if config.Connection.RetryDelay < 5 {
		return errors.New("Connection Retry Delay must be at least 5 seconds")
	}

	if len(config.Queue.Name) == 0 {
		return errors.New("Queue Name is empty or missing")
	}

	if config.Queue.WaitDelay < 1 {
		return errors.New("Queue Wait Delay must be at least 1 second")
	}

	if config.Queue.PrefetchCount < 1 {
		return errors.New("PrefetchCount cannot be negative")
	}

	if config.Message.DefaultTTL < 1 {
		return errors.New("Message Default TTL must be at least 1 second")
	}

	if config.Http.Timeout < 5 {
		return errors.New("Http Timeout must be at least 5 seconds")
	}

	if len(config.Log.LogFile) == 0 {
		return errors.New("LogFile path is empty or missing")
	}

	return nil
}

func (config *ConfigParameters) String() string {
	cfgDtls := ""
	cfgDtls += "[Connection]\n"
	cfgDtls += "  RabbitmqURL = \"" + config.Connection.RabbitmqURL + "\"\n"
	cfgDtls += "  RetryDelay = " + strconv.Itoa(config.Connection.RetryDelay) + "\n"
	cfgDtls += "[Queue]\n"
	cfgDtls += "  Name = \"" + config.Queue.Name + "\"\n"
	cfgDtls += "  WaitDelay = " + strconv.Itoa(config.Queue.WaitDelay) + "\n"
	cfgDtls += "  PrefetchCount = " + strconv.Itoa(config.Queue.PrefetchCount) + "\n"
	cfgDtls += "[Message]\n"
	cfgDtls += "  DefaultTTL = " + strconv.Itoa(config.Message.DefaultTTL) + "\n"
	cfgDtls += "[Http]\n"
	cfgDtls += "  Timeout = " + strconv.Itoa(config.Http.Timeout) + "\n"
	cfgDtls += "[Log]\n"
	cfgDtls += "  LogFile = \"" + config.Log.LogFile + "\""

	return cfgDtls
}
