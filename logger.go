package wrapbit

import (
	"context"
	"fmt"
	"log/slog"
)

const (
	levelError level = iota
	levelWarn
	levelInfo
	levelDebug
)

type level int

type Logger interface {
	Debug(args ...any)
	Error(args ...any)
	Info(args ...any)
	Warn(args ...any)
}

type debugLogger struct{}

func (l *debugLogger) Debug(args ...any) {
	l.log(context.Background(), slog.LevelDebug, args)
}

func (l *debugLogger) Error(args ...any) {
	l.log(context.Background(), slog.LevelError, args)
}

func (l *debugLogger) Info(args ...any) {
	l.log(context.Background(), slog.LevelInfo, args)
}

func (l *debugLogger) setLevel(level level) {
	var slogLevel slog.Level

	switch level {
	case levelDebug:
		slogLevel = slog.LevelDebug
	case levelInfo:
		slogLevel = slog.LevelInfo
	case levelWarn:
		slogLevel = slog.LevelWarn
	case levelError:
		slogLevel = slog.LevelError
	default:
		slogLevel = slog.LevelError
	}

	slog.SetLogLoggerLevel(slogLevel)
}

func (l *debugLogger) Warn(args ...any) {
	l.log(context.Background(), slog.LevelWarn, args)
}

func (l *debugLogger) log(ctx context.Context, level slog.Level, args ...any) {
	switch len(args) {
	case 0:
		slog.Log(ctx, level, "")
	case 1:
		slog.Log(ctx, level, fmt.Sprint(args[0]))
	default:
		slog.Log(ctx, level, fmt.Sprint(args[0]), args[1:])
	}
}

type nullLogger struct{}

func (l *nullLogger) Debug(_ ...any) {}

func (l *nullLogger) Error(_ ...any) {}

func (l *nullLogger) Info(_ ...any) {}

func (l *nullLogger) Warn(_ ...any) {}
