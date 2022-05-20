package isaacstates

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSyncer struct {
	isaac.BaseTestBallots
}

func (t *testSyncer) TestNew() {
	s := NewSyncer(nil, nil, nil)

	_ = (interface{})(s).(isaac.Syncer)
}

func (t *testSyncer) maps(from, to base.Height) []base.BlockMap {
	maps := make([]base.BlockMap, (to - from + 1).Int64())

	var previous, previousSuffrage util.Hash
	for i := from; i <= to; i++ {
		m, err := newTestBlockMap(i, previous, previousSuffrage, t.Local, t.NodePolicy.NetworkID())
		t.NoError(err)

		maps[(i - from).Int64()] = m
		previous = m.Manifest().Hash()
		previousSuffrage = m.Manifest().Suffrage()
	}

	return maps
}

func (t *testSyncer) TestAdd() {
	t.Run("with nil last", func() {
		s := NewSyncer(nil, nil, nil)

		height := base.Height(33)
		t.True(s.Add(height))
		t.Equal(height, s.Top())
	})

	t.Run("same with last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(last, nil, nil)

		height := base.Height(33)
		t.False(s.Add(height))
		t.Equal(lastheight, s.Top())
	})

	t.Run("older than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(last, nil, nil)

		height := lastheight - 1
		t.False(s.Add(height))
		t.Equal(lastheight, s.Top())
	})

	t.Run("higher than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(last, nil, nil)

		height := lastheight + 1
		t.True(s.Add(height))
		t.Equal(height, s.Top())
	})
}

func (t *testSyncer) TestAddChan() {
	t.Run("with nil last", func() {
		s := NewSyncer(nil, nil, nil)

		height := base.Height(33)
		t.True(s.Add(height))
		t.Equal(height, s.Top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.addch:
			t.Equal(h, height)
		}
	})

	t.Run("with last", func() {
		lastheight := base.Height(33)

		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(last, nil, nil)

		height := lastheight + 1
		t.True(s.Add(height))
		t.Equal(height, s.Top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.addch:
			t.Equal(h, height)
		}
	})

	t.Run("same with synced height", func() {
		s := NewSyncer(nil, nil, nil)

		s.topLocked = util.NewLocked(base.Height(33))
		s.syncedheightLocked = util.NewLocked(base.Height(33))

		height := base.Height(34)
		t.True(s.Add(height))
		t.Equal(height, s.Top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.addch:
			t.Equal(h, height)
		}
	})
}

func (t *testSyncer) TestCancel() {
	s := NewSyncer(nil, nil, nil)
	t.NoError(s.Start())

	t.NoError(s.Cancel())
	t.NoError(s.Cancel())
}

func (t *testSyncer) TestFetchMaps() {
	t.Run("fetch error", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		s := NewSyncer(nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			if index == 1 {
				return nil, false, errors.Errorf("hehehe")
			}

			return maps[index], true, nil
		}, nil)
		t.NoError(s.Start())
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.Error(s.Err())
			t.True(errors.Is(s.Err(), context.Canceled) || strings.Contains(s.Err().Error(), "hehehe"))
		}
	})

	t.Run("validation error", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)
		wrongmap := t.maps(maps[1].Manifest().Height(), maps[1].Manifest().Height())[0]
		maps[1] = wrongmap

		s := NewSyncer(nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil)
		t.NoError(s.Start())
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.Error(s.Err())

			t.True(errors.Is(s.Err(), context.Canceled) || strings.Contains(s.Err().Error(), "previous does not match"))
		}
	})

	t.Run("with nil last", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		s := NewSyncer(nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil)
		t.NoError(s.Start())
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.NoError(s.Err())
		}
	})

	t.Run("with last", func() {
		lastheight := base.Height(3)
		to := lastheight + 5
		maps := t.maps(lastheight, to)

		s := NewSyncer(maps[0], func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - lastheight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil)
		t.NoError(s.Start())
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.NoError(s.Err())
		}
	})

	t.Run("over batchlimit", func() {
		lastheight := base.Height(3)
		to := lastheight + 5
		maps := t.maps(lastheight, to)

		s := NewSyncer(maps[0], func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - lastheight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil)
		s.batchlimit = 2
		t.NoError(s.Start())
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.NoError(s.Err())
		}
	})
}

func TestSyncer(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSyncer))
}
