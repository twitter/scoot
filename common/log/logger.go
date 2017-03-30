package log

import (
	"github.com/Sirupsen/logrus"
)

var Log = logrus.New()

func AddHook(hook logrus.Hook) {
	logrus.AddHook(hook)
}

func Debug(args ...interface{}) {
	Log.Debug(args)
}

func Debugf(format string, args ...interface{}) {
	Log.Debugf(format, args)
}

func Debugln(args ...interface{}) {
	Log.Debugln(args)
}

func Error(args ...interface{}) {
	Log.Error(args)
}

func Errorf(format string, args ...interface{}) {
	Log.Errorf(format, args)
}

func Errorln(args ...interface{}) {
	Log.Errorln(args)
}

func Info(args ...interface{}) {
	Log.Info(args)
}

func Infof(format string, args ...interface{}) {
	Log.Infof(format, args)
}

func Infoln(args ...interface{}) {
	Log.Infoln(args)
}
