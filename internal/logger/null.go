package logger

type NullLogger struct{}

func (l *NullLogger) Debug(_ ...any) {}

func (l *NullLogger) Error(_ ...any) {}

func (l *NullLogger) Info(_ ...any) {}

func (l *NullLogger) Warn(_ ...any) {}
