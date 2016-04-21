// Package logfile provides convenience functions for logging to a file with an ISO 8601 datetime prefix.
// Capabilities include Reopen to reopen a file after log rotation, and graceful handling of fatal logging errors.
package logfile

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"time"
)

type Logger struct {
	filepath   string
	fp         *os.File
	debugMode  bool
	fatalError bool
}

func New(file string, debug bool) (Logger, error) {
	logger := Logger{
		filepath:   file,
		fp:         nil,
		debugMode:  debug,
		fatalError: true,
	}

	if len(file) == 0 {
		return logger, errors.New("Log filename not provided")
	}

	err := logger.open()
	if err != nil {
		return logger, errors.New("Could not open log file: " + logger.filepath + " - " + err.Error())
	}

	return logger, nil
}

func (l *Logger) open() error {
	l.fatalError = true
	l.fp = nil

	fp, err := os.OpenFile(l.filepath, os.O_CREATE+os.O_WRONLY+os.O_APPEND, 0644)

	if err == nil {
		l.fp = fp
		l.fatalError = false
	}

	return err
}

func (l *Logger) Write(args ...interface{}) error {
	if l.fatalError {
		return errors.New("Fatal error previously encountered on log file: " + l.filepath)
	}

	var line bytes.Buffer

	// Get current time in ISO 8601 format
	line.WriteString(time.Now().Format("2006-01-02T15:04:05Z07:00"))

	line.WriteString(" - ")

	line.WriteString(fmt.Sprintln(args...))

	if _, err := l.fp.WriteString(line.String()); err != nil {
		l.Close()
		l.fatalError = true
		return errors.New("Cannot write to log file: " + l.filepath + " - " + err.Error())
	}

	return nil
}

func (l *Logger) WriteDebug(args ...interface{}) error {
	if l.debugMode {
		return l.Write(args...)
	}

	return nil
}

func (l *Logger) Close() error {
	if l.fp == nil {
		return nil
	}

	fp := l.fp
	l.fp = nil

	if err := fp.Close(); err != nil {
		return errors.New("Error encountered when closing log file: " + l.filepath + " - " + err.Error())
	}

	return nil
}

func (l *Logger) Reopen() error {
	l.Close()
	return l.open()
}

func (l Logger) HasFatalError() bool {
	return l.fatalError
}
