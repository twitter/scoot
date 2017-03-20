/**
Utility to get the service's logger.  All objects will use GetLogger() to get the logger.
We are currently using golang's default logger and setting it to write to log.txt in
a directory created by temp.TempDirDefault()

To add a new logging level <levelX>:
1. add the <levelX> to the const () declaration
2. declare a logger for that levelX (see bottom of file)
3. declare LevelX() and LevelXF() functions matching the Info() and Infof() pattern

*/
package logger

import (
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/scootdev/scoot/os/temp"
	"runtime/debug"
	"strings"
)

const (
	FATAL_LEVEL = iota
	TRACE_LEVEL
	INFO_LEVEL
)

var mu = &sync.Mutex{}

/**
Set the log level.  Only messages at or below this level will be printed
*/
func SetLevel(l int) {
	level = l
}

/**
functions for printing the different levels.  If we add Debug, Warn etc. add corresponding
methods here
*/
func Info(a ...interface{}) {
	doPrint(INFO_LEVEL, infoLogger, "", a...)
}

func Infof(format string, v ...interface{}) {
	doPrint(INFO_LEVEL, infoLogger, format, v...)
}

func Trace(a ...interface{}) {
	doPrint(TRACE_LEVEL, traceLogger, "", a...)
}

func Tracef(format string, v ...interface{}) {
	doPrint(TRACE_LEVEL, traceLogger, format, v...)
}

func Fatal(a ...interface{}) {
	doPrint(FATAL_LEVEL, fatalLogger, "", a...)
}

func Fatalf(format string, v ...interface{}) {
	doPrint(FATAL_LEVEL, fatalLogger, format, v...)
}

func doPrint(l int, logger *log.Logger, format string, v ...interface{}) {
	if level >= l {
		logLine := getLogLine()
		mu.Lock()
		defer mu.Unlock()
		prefix := logger.Prefix()
		logger.SetPrefix(prefix + logLine + ";")
		if format != "" {
			logger.Printf(format, v...)

		} else {
			logger.Print(v...)
		}
		logger.SetPrefix(prefix)
	}
}

/**
get the stack trace line that issued the logger call
*/
func getLogLine() string {
	stack := debug.Stack()
	lines := strings.Split(string(stack), "\n")
	foundLoggerBlock := false
	incr := 1
	for i := 0; i < len(lines); i = i + incr {
		if strings.Contains(lines[i], "logger.go:") {
			foundLoggerBlock = true
			incr = 2
			continue
		}
		if !foundLoggerBlock {
			continue
		}
		return lines[i]
	}
	return ""
}

var level int = INFO_LEVEL

/*
the writer is defined here
*/
type logWriter struct {
	machineName string
	filename    string
	file        *os.File
}

var lw = getDefaultLogWriter()

/*
This section of the code
creates a log writer that writes to log.txt (in the directory created by temp.TempDirDefault().

We can extend this later to return a writer that implements configurable behavior (eg: specified in a
property file).  (example: rolling writes, writing to designated file name, screening undesired log
messages, etc.
*/
func getDefaultLogWriter() *logWriter {
	var tmpDir, err = temp.MakeTempDir(temp.Scoot_tmp_dir_prefix + "log-")
	if err != nil {
		log.Panicf("Logger initialization failed creating defaultLogWriter, getting temp dir:%s", err.Error())
	}

	logFilename := tmpDir.Dir + "/log.txt"
	file, err := os.OpenFile(logFilename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		log.Panicf("Logger initialization failed opening log file '%s': %s", logFilename, err.Error())
	}

	hostname, err := os.Hostname()
	if err != nil {
		log.Panicf("Logger initialization failed getting hostname: %s", err.Error())
	}

	lw := logWriter{filename: logFilename, file: file, machineName: hostname}
	return &lw
}

/**
write a new line to the log file (automatically append \n)
*/
func (w *logWriter) Write(message []byte) (n int, err error) {
	t := fmt.Sprintf("%s;%s", w.machineName, string(message))
	return w.file.WriteString(t)
}

/**
Returns the filename that the logger is currently using.  (Intended for validation only.)
*/
func getLogFilename() string {
	return lw.filename
}

/**
these are the actual loggers with a preset prefix.  They will all write to the same writer.
*/
var infoLogger = log.New(lw, "INFO;", log.LstdFlags|log.Lmicroseconds|log.LUTC)
var traceLogger = log.New(lw, "TRACE;", log.LstdFlags|log.Lmicroseconds|log.LUTC)
var fatalLogger = log.New(lw, "FATAL;", log.LstdFlags|log.Lmicroseconds|log.LUTC)
