package launch

import (
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"golang.org/x/sync/singleflight"
)

type SuffragePool struct {
	byHeightFunc func(base.Height) (base.Suffrage, bool, error)
	lastf        func() (base.Height, base.Suffrage, bool, error)
	cache        *util.GCache[string, base.Suffrage]
	sg           singleflight.Group
	expire       time.Duration
	sync.RWMutex
}

func NewSuffragePool(
	byHeightf func(base.Height) (base.Suffrage, bool, error),
	lastf func() (base.Height, base.Suffrage, bool, error),
) *SuffragePool {
	return &SuffragePool{
		byHeightFunc: byHeightf,
		lastf:        lastf,
		cache:        util.NewLRUGCache[string, base.Suffrage](1 << 9), //nolint:gomnd //...
		expire:       time.Second * 3,                                  //nolint:gomnd //...
	}
}

func (s *SuffragePool) Height(height base.Height) (base.Suffrage, bool, error) {
	s.RLock()
	defer s.RUnlock()

	return s.get(
		height,
		s.byHeightFunc,
	)
}

func (s *SuffragePool) Last() (base.Suffrage, bool, error) {
	s.RLock()
	defer s.RUnlock()

	switch i, err, _ := s.sg.Do("last", func() (interface{}, error) {
		switch height, i, found, err := s.lastf(); {
		case err != nil, !found:
			return nil, err
		case i == nil:
			return nil, nil
		default:
			return [2]interface{}{height, i}, nil
		}
	}); {
	case err != nil:
		return nil, false, errors.WithStack(err)
	case i == nil:
		return nil, false, nil
	default:
		j := i.([2]interface{}) //nolint:forcetypeassert //...

		height := j[0].(base.Height) //nolint:forcetypeassert //...
		suf := j[1].(base.Suffrage)  //nolint:forcetypeassert //...

		s.cache.Set(height.String(), suf, s.expire)

		return suf, true, nil
	}
}

func (s *SuffragePool) Purge() {
	s.Lock()
	defer s.Unlock()

	_, _, _ = s.sg.Do("last", func() (interface{}, error) {
		s.cache.Purge()

		return nil, nil
	})
}

func (s *SuffragePool) get(
	height base.Height,
	f func(base.Height) (base.Suffrage, bool, error),
) (base.Suffrage, bool, error) {
	i, err, _ := s.sg.Do(height.String(), func() (interface{}, error) {
		if i, found := s.cache.Get(height.String()); found {
			return [2]interface{}{i, true}, nil //nolint:forcetypeassert //...
		}

		switch suf, found, err := f(height); {
		case err != nil || !found:
			return [2]interface{}{nil, found}, err
		default:
			s.cache.Set(height.String(), suf, s.expire)

			return [2]interface{}{suf, true}, nil
		}
	})
	if err != nil {
		return nil, false, errors.WithStack(err)
	}

	found := i.([2]interface{})[1].(bool) //nolint:forcetypeassert //...
	if !found {
		return nil, false, nil
	}

	return i.([2]interface{})[0].(base.Suffrage), true, nil //nolint:forcetypeassert //...
}
