// Wrapper for github.com/inconshreveable/log15
// Allows configurable handlers for users of scoot
// log15 handler docs are available at:
// https://godoc.org/github.com/inconshreveable/log15#Handler
package logger

import log "github.com/inconshreveable/log15"

var Log = log.New()

func Info(msg string, ctx ...interface{}) {
	Log.Info(msg, ctx)
}

func Debug(msg string, ctx ...interface{}) {
	Log.Debug(msg, ctx)
}

func Crit(msg string, ctx ...interface{}) {
	Log.Crit(msg, ctx)
}
