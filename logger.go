package magneticdb

import (
	"log"
	"io"
)

// Logger provides
type Logger struct {
	trace   *log.Logger
	info    *log.Logger
	warning *log.Logger
	errorm  *log.Logger
	start   bool
}

// Logger config provides configuration for logger
type LoggerConfig struct {
	TraceHandle   io.Writer
	InfoHandle    io.Writer
	WarningHandle io.Writer
	ErrorHandle   io.Writer
}


// NewLOgger provides configuration for logger object
func NewLogger(cfg *LoggerConfig) *Logger {
	logger := new(Logger)
	if cfg == nil {
		logger.start = false
		return logger
	}
	logger.trace = log.New(cfg.TraceHandle,
		"TRACE: ",
		log.Ldate|log.Ltime)

	logger.info = log.New(cfg.InfoHandle,
		"INFO: ",
		log.Ldate|log.Ltime)

	logger.warning = log.New(cfg.WarningHandle,
		"WARNING: ",
		log.Ldate|log.Ltime)

	logger.errorm = log.New(cfg.ErrorHandle,
		"ERROR: ",
		log.Ldate|log.Ltime)
	logger.start = true
	return logger
}

func (logger *Logger) Info(msg string) {
	if logger.start {
		logger.info.Println(msg)
	}
}

func (logger *Logger) Trace(msg string) {
	if logger.start {
		logger.trace.Println(msg)
	}
}

func (logger *Logger) Warning(msg string) {
	if logger.start {
		logger.warning.Println(msg)
	}
}

func (logger *Logger) Error(msg string) {
	if logger.start {
		logger.errorm.Println(msg)
	}
}
