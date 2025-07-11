package logger

import (
	"context"
	"fmt"
	"log/slog"
)

type Logger struct{}

func (l *Logger) Debug(args ...any) {
	l.log(context.Background(), slog.LevelDebug, args)
}

func (l *Logger) Error(args ...any) {
	l.log(context.Background(), slog.LevelError, args)
}

func (l *Logger) Info(args ...any) {
	l.log(context.Background(), slog.LevelInfo, args)
}

func (l *Logger) Warn(args ...any) {
	l.log(context.Background(), slog.LevelWarn, args)
}

func (l *Logger) log(ctx context.Context, level slog.Level, args ...any) {
	switch len(args) {
	case 0:
		slog.Log(ctx, level, "")
	case 1:
		slog.Log(ctx, level, fmt.Sprint(args[0]))
	default:
		slog.Log(ctx, level, fmt.Sprint(args[0]), args[1:])
	}
}
