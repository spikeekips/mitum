package util

import (
	"time"
)

var StopTimerError = NewError("stop timer")

type TimerID string

func (ti TimerID) String() string {
	return string(ti)
}

type Timer interface {
	Daemon
	IsStarted() bool
	ID() TimerID
	SetInterval(func(int) time.Duration) Timer
	Reset() error
}
