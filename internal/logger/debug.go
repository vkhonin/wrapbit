package logger

import (
	"context"
	"fmt"
	"log/slog"
)

const (
	LevelError Level = iota
	LevelWarn
	LevelInfo
	LevelDebug
)

type DebugLogger struct{}

type Level int

func (l *DebugLogger) Debug(args ...any) {
	l.log(context.Background(), slog.LevelDebug, args)
}

func (l *DebugLogger) Error(args ...any) {
	l.log(context.Background(), slog.LevelError, args)
}

func (l *DebugLogger) Info(args ...any) {
	l.log(context.Background(), slog.LevelInfo, args)
}

func (l *DebugLogger) SetLevel(level Level) {
	var slogLevel slog.Level

	switch level {
	case LevelDebug:
		slogLevel = slog.LevelDebug
	case LevelInfo:
		slogLevel = slog.LevelInfo
	case LevelWarn:
		slogLevel = slog.LevelWarn
	case LevelError:
	default:
		slogLevel = slog.LevelError
	}

	slog.SetLogLoggerLevel(slogLevel)
}

func (l *DebugLogger) Warn(args ...any) {
	l.log(context.Background(), slog.LevelWarn, args)
}

func (l *DebugLogger) log(ctx context.Context, level slog.Level, args ...any) {
	switch len(args) {
	case 0:
		slog.Log(ctx, level, "")
	case 1:
		slog.Log(ctx, level, fmt.Sprint(args[0]))
	default:
		slog.Log(ctx, level, fmt.Sprint(args[0]), args[1:])
	}
}
