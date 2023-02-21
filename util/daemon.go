package util

import "context"

var (
	ErrDaemonAlreadyStarted = NewMError("daemon already started")
	ErrDaemonAlreadyStopped = NewMError("daemon already stopped")
)

type Daemon interface {
	Start(context.Context) error
	Stop() error
}
