// Wrapper for github.com/inconshreveable/log15
// Allows configurable handlers for users of Scoot logging
// log15 handler docs are available at:
// https://godoc.org/github.com/inconshreveable/log15#Handler
//
// Ex. For logging file & line # to stdout,
//
// import (
// 		log "github.com/scootdev/scoot/common/logger"
// 		"github.com/inconshreveable/log15"
// )
//
// h := log15.CallerFileHandler(log15.StdoutHandler)
// log.SetHandler(h)

package logger

import log "github.com/inconshreveable/log15"

var Log = log.New()

// New returns a new Logger that has this logger's context plus the given context
func New(ctx ...interface{}) log.Logger {
	return log.New(ctx)
}

// GetHandler gets the handler associated with the logger.
func GetHandler() log.Handler {
	return Log.GetHandler()
}

// SetHandler updates the logger to write records to the specified handler.
func SetHandler(h log.Handler) {
	Log.SetHandler(h)
}

// Log a message at the given level with context key/value pairs
func Debug(msg string, ctx ...interface{}) {
	Log.Debug(msg, ctx)
}

func Info(msg string, ctx ...interface{}) {
	Log.Info(msg, ctx)
}

func Crit(msg string, ctx ...interface{}) {
	Log.Crit(msg, ctx)
}
