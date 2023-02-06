package pika

type Logger interface {
	Debug(...any)
	Info(...any)
	Warn(...any)
	Error(...any)
}

type nullLogger struct{}

func (l nullLogger) Debug(args ...any) {}
func (l nullLogger) Info(args ...any)  {}
func (l nullLogger) Warn(args ...any)  {}
func (l nullLogger) Error(args ...any) {}
