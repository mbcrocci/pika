package pika

import "testing"

type Logger interface {
	Debug(...any)
	Info(...any)
	Warn(...any)
	Error(...any)
}

type testLogger struct {
	t *testing.T
}

func (l testLogger) Debug(args ...any) { l.t.Log(args...) }
func (l testLogger) Info(args ...any)  { l.t.Log(args...) }
func (l testLogger) Warn(args ...any)  { l.t.Log(args...) }
func (l testLogger) Error(args ...any) { l.t.Log(args...) }
