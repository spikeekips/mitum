package util

import "context"

var (
	ErrDaemonAlreadyStarted = NewIDError("daemon already started")
	ErrDaemonAlreadyStopped = NewIDError("daemon already stopped")
)

type Daemon interface {
	Start(context.Context) error
	Stop() error
}
