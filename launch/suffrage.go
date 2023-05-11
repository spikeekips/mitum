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
	sg           *singleflight.Group
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
		sg:           &singleflight.Group{},
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

	switch i, err, _ := util.SingleflightDo[[2]interface{}](s.sg, "last", func() ([2]interface{}, error) {
		switch height, i, found, err := s.lastf(); {
		case err != nil, !found:
			return [2]interface{}{}, err
		case i == nil:
			return [2]interface{}{}, nil
		default:
			return [2]interface{}{height, i}, nil
		}
	}); {
	case err != nil:
		return nil, false, errors.WithStack(err)
	case i[1] == nil:
		return nil, false, nil
	default:
		height := i[0].(base.Height) //nolint:forcetypeassert //...
		suf := i[1].(base.Suffrage)  //nolint:forcetypeassert //...

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
	i, err, _ := util.SingleflightDo(s.sg, height.String(), func() ([2]interface{}, error) {
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

	found := i[1].(bool) //nolint:forcetypeassert //...
	if !found {
		return nil, false, nil
	}

	return i[0].(base.Suffrage), true, nil //nolint:forcetypeassert //...
}
