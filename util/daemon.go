package util

var (
	ErrDaemonAlreadyStarted = NewError("daemon already started")
	ErrDaemonAlreadyStopped = NewError("daemon already stopped")
)

type Daemon interface {
	Start() error
	Stop() error
}
