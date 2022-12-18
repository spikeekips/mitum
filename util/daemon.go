package util

import "context"

var (
	ErrDaemonAlreadyStarted = NewError("daemon already started")
	ErrDaemonAlreadyStopped = NewError("daemon already stopped")
)

type Daemon interface {
	Start(context.Context) error
	Stop() error
}
