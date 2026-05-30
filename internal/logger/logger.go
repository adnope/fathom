package logger

import (
	"log/slog"
	"os"
	"strings"
)

var (
	logLevelVar = new(slog.LevelVar)
)

func Init(level string) *slog.Logger {
	SetLevel(level)

	options := &slog.HandlerOptions{
		Level: logLevelVar,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				a.Key = "ts"
			}
			if a.Key == slog.MessageKey {
				a.Key = "event"
			}
			if a.Key == slog.LevelKey {
				a.Value = slog.StringValue(strings.ToLower(a.Value.String()))
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(os.Stdout, options)
	l := slog.New(handler)
	slog.SetDefault(l)
	return l
}

func SetLevel(level string) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}
	logLevelVar.Set(lvl)
}
