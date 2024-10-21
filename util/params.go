package util

import (
	"sync"
	"time"

	"github.com/pkg/errors"
)

type BaseParams struct {
	id string
	l  sync.RWMutex
}

func NewBaseParams() *BaseParams {
	return &BaseParams{
		id: UUID().String(),
	}
}

func (p *BaseParams) IsValid([]byte) error {
	if len(p.id) < 1 {
		return errors.Errorf("empty id")
	}

	return nil
}

func (p *BaseParams) ID() string {
	p.l.RLock()
	defer p.l.RUnlock()

	return p.id
}

func (p *BaseParams) Set(f func() (bool, error)) error {
	p.l.Lock()
	defer p.l.Unlock()

	switch updated, err := f(); {
	case err != nil:
		return err
	case !updated:
		return nil
	default:
		p.id = UUID().String()

		return nil
	}
}

func (p *BaseParams) SetDuration(d time.Duration, f func(time.Duration) (bool, error)) error {
	return p.Set(func() (bool, error) {
		switch {
		case d < 1:
			return false, errors.Errorf("under zero")
		default:
			return f(d)
		}
	})
}

func (p *BaseParams) SetUint64(d uint64, f func(uint64) (bool, error)) error {
	return p.Set(func() (bool, error) {
		switch {
		case d < 1:
			return false, errors.Errorf("under zero")
		default:
			return f(d)
		}
	})
}

func (p *BaseParams) SetInt(d int, f func(int) (bool, error)) error {
	return p.Set(func() (bool, error) {
		return f(d)
	})
}

func (p *BaseParams) SetOverZeroInt(d int, f func(int) (bool, error)) error {
	return p.Set(func() (bool, error) {
		switch {
		case d < 1:
			return false, errors.Errorf("under 1")
		default:
			return f(d)
		}
	})
}
