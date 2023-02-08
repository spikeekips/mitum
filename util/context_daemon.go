package util

import (
	"context"
	"sync"
)

type ContextDaemon struct {
	ctxDone            func()
	callback           func(context.Context) error
	callbackCancelFunc func()
	stopfunc           func()
	ctxLock            sync.RWMutex
	sync.RWMutex
}

func NewContextDaemon(startfunc func(context.Context) error) *ContextDaemon {
	return &ContextDaemon{
		callback: startfunc,
	}
}

func (dm *ContextDaemon) IsStarted() bool {
	dm.ctxLock.RLock()
	defer dm.ctxLock.RUnlock()

	return dm.callbackCancelFunc != nil
}

func (dm *ContextDaemon) Start(ctx context.Context) error {
	if dm.IsStarted() {
		return ErrDaemonAlreadyStarted.Call()
	}

	_ = dm.Wait(ctx)

	return nil
}

func (dm *ContextDaemon) Wait(ctx context.Context) <-chan error {
	dm.Lock()
	defer dm.Unlock()

	ch := make(chan error, 1)

	if dm.IsStarted() {
		go func() {
			ch <- ErrDaemonAlreadyStarted
		}()

		return ch
	}

	nctx, _, _, finish := dm.getCtx(ctx)

	go func() {
		err := dm.callback(nctx)

		finish()
		dm.releaseCallbackCtx()

		ch <- err
		close(ch)
	}()

	return ch
}

func (dm *ContextDaemon) Stop() error {
	dm.Lock()
	defer dm.Unlock()

	if !dm.IsStarted() {
		return ErrDaemonAlreadyStopped.Call()
	}

	dm.callbackCancel()
	dm.waitCallbackFinished()
	dm.releaseCallbackCtx()

	return nil
}

func (dm *ContextDaemon) getCtx(ctx context.Context) (context.Context, func(), func(), func()) {
	dm.ctxLock.Lock()
	defer dm.ctxLock.Unlock()

	callbackCtx, callbackCancelFunc := context.WithCancel(ctx)
	dm.callbackCancelFunc = callbackCancelFunc

	nctx, stopfunc := context.WithCancel(context.Background())
	dm.ctxDone = func() {
		<-nctx.Done()
	}

	dm.stopfunc = stopfunc

	return callbackCtx, dm.callbackCancelFunc, dm.ctxDone, dm.stopfunc
}

func (dm *ContextDaemon) releaseCallbackCtx() {
	dm.ctxLock.Lock()
	defer dm.ctxLock.Unlock()

	dm.callbackCancelFunc = nil
}

func (dm *ContextDaemon) callbackCancel() {
	dm.ctxLock.RLock()
	defer dm.ctxLock.RUnlock()

	if dm.callbackCancelFunc != nil {
		dm.callbackCancelFunc()
	}
}

func (dm *ContextDaemon) waitCallbackFinished() {
	dm.ctxLock.RLock()
	defer dm.ctxLock.RUnlock()

	dm.ctxDone()
}
