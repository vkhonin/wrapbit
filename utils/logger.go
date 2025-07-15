package utils

type Logger interface {
	Debug(args ...any)
	Error(args ...any)
	Info(args ...any)
	Warn(args ...any)
}
