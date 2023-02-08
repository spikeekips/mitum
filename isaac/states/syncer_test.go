package isaacstates

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testSyncer struct {
	isaac.BaseTestBallots
	isaacdatabase.BaseTestDatabase
}

func (t *testSyncer) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaacblock.BlockMapHint, Instance: isaacblock.BlockMap{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTVoteproofHint, Instance: isaac.ACCEPTVoteproof{}}))

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.OperationFixedtreeHint, Instance: base.OperationFixedtreeNode{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.StateFixedtreeHint, Instance: fixedtree.BaseNode{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotSignFactHint, Instance: isaac.ACCEPTBallotSignFact{}}))
}

func (t *testSyncer) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testSyncer) TestNew() {
	s := NewSyncer(nil, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

	_ = (interface{})(s).(isaac.Syncer)
}

func (t *testSyncer) maps(from, to base.Height) []base.BlockMap {
	maps := make([]base.BlockMap, (to - from + 1).Int64())

	var previous, previousSuffrage util.Hash
	for i := from; i <= to; i++ {
		m, err := newTestBlockMap(i, previous, previousSuffrage, t.Local, t.LocalParams.NetworkID())
		t.NoError(err)

		maps[(i - from).Int64()] = m
		previous = m.Manifest().Hash()
		previousSuffrage = m.Manifest().Suffrage()
	}

	return maps
}

func (t *testSyncer) dummyNewBlockImporterFunc() NewBlockImporterFunc {
	return func(base.BlockMap) (isaac.BlockImporter, error) {
		return &isaacblock.DummyBlockImporter{}, nil
	}
}

func (t *testSyncer) dummyBlockMapItemFunc() SyncerBlockMapItemFunc {
	return func(context.Context, base.Height, base.BlockMapItemType) (io.ReadCloser, func() error, bool, error) {
		return io.NopCloser(&bytes.Buffer{}), func() error { return nil }, true, nil
	}
}

func (t *testSyncer) dummySetLastVoteproofs() func(isaac.BlockReader) error {
	return func(isaac.BlockReader) error {
		return nil
	}
}

func (t *testSyncer) TestAdd() {
	t.Run("with nil last", func() {
		s := NewSyncer(nil, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := base.Height(33)
		t.True(s.Add(height))
		t.Equal(height, s.top())

		<-s.startsyncch
	})

	t.Run("same with last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(nil, last, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := base.Height(33)
		t.False(s.Add(height))
		t.Equal(lastheight, s.top())
	})

	t.Run("older than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(nil, last, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := lastheight - 1
		t.False(s.Add(height))
		t.Equal(lastheight, s.top())
	})

	t.Run("higher than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(nil, last, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := lastheight + 1
		t.True(s.Add(height))
		t.Equal(height, s.top())

		<-s.startsyncch
	})
}

func (t *testSyncer) TestAddChan() {
	t.Run("with nil last", func() {
		s := NewSyncer(nil, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := base.Height(33)
		t.True(s.Add(height))
		t.Equal(height, s.top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.startsyncch:
			t.Equal(h, height)
		}
	})

	t.Run("with last", func() {
		lastheight := base.Height(33)

		last := t.maps(lastheight, lastheight)[0]

		s := NewSyncer(nil, last, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		height := lastheight + 1
		t.True(s.Add(height))
		t.Equal(height, s.top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.startsyncch:
			t.Equal(h, height)
		}
	})

	t.Run("same with synced height", func() {
		s := NewSyncer(nil, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)

		s.topvalue = util.NewLocked(base.Height(33))
		s.prevvalue.SetValue(t.maps(base.Height(33), base.Height(33))[0])

		height := base.Height(34)
		t.True(s.Add(height))
		t.Equal(height, s.top())

		select {
		case <-time.After(time.Millisecond * 300):
			t.NoError(errors.Errorf("waits height from addch, but not"))
		case h := <-s.startsyncch:
			t.Equal(h, height)
		}
	})
}

func (t *testSyncer) TestCancel() {
	stopped := util.UUID().String()
	stoppedch := make(chan string, 1)

	s := NewSyncer(nil, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool(), nil, func() error {
		stoppedch <- stopped

		return nil
	}, nil, nil)
	t.NoError(s.Start(context.Background()))

	t.NoError(s.Cancel())
	t.NoError(s.Cancel())

	select {
	case <-time.After(time.Second):
		t.NoError(errors.Errorf("waits whenStopped, but failed"))
	case r := <-stoppedch:
		t.Equal(stopped, r)
	}
}

func (t *testSyncer) TestFetchMaps() {
	t.Run("fetch error", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		s := NewSyncer(nil, nil, nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			if index == 1 {
				return nil, false, errors.Errorf("hehehe")
			}

			return maps[index], true, nil
		}, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)
		t.NoError(s.Start(context.Background()))
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

		s := NewSyncer(nil, nil, nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil, isaacdatabase.NewMemTempSyncPool(), nil, nil, nil, nil)
		t.NoError(s.Start(context.Background()))
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

	t.Run("top updated", func() {
		to := base.Height(5)
		newto := to + 5
		maps := t.maps(base.GenesisHeight, newto)

		reachedlock := sync.NewCond(&sync.Mutex{})

		reachedch := make(chan struct{})
		var reached bool
		s := NewSyncer(
			t.dummyNewBlockImporterFunc(),
			nil,
			nil,
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				reachedlock.L.Lock()
				if height == to && !reached {
					close(reachedch)

					reachedlock.Wait()
				}
				reachedlock.L.Unlock()

				index := (height - base.GenesisHeight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
			t.dummySetLastVoteproofs(),
			nil,
			nil,
			nil,
		)
		t.NoError(s.Start(context.Background()))
		defer s.Cancel()

		t.True(s.Add(to))
		<-reachedch

		reachedlock.L.Lock()
		reached = true
		t.True(s.Add(newto))
		reachedlock.L.Unlock()
		reachedlock.Broadcast()

		select {
		case height := <-s.Finished():
			t.Equal(newto, height)
		case <-s.Done():
			t.NoError(s.Err())
		}
	})

	t.Run("with nil last", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		s := NewSyncer(
			t.dummyNewBlockImporterFunc(),
			nil,
			nil,
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				index := (height - base.GenesisHeight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
			t.dummySetLastVoteproofs(),
			nil,
			nil,
			nil,
		)
		t.NoError(s.Start(context.Background()))
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

		s := NewSyncer(
			t.dummyNewBlockImporterFunc(),
			maps[0],
			nil,
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				index := (height - lastheight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
			t.dummySetLastVoteproofs(),
			nil,
			nil,
			nil,
		)
		t.NoError(s.Start(context.Background()))
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

		s := NewSyncer(
			t.dummyNewBlockImporterFunc(),
			maps[0],
			nil,
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				index := (height - lastheight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
			t.dummySetLastVoteproofs(),
			nil,
			nil,
			nil,
		)

		s.batchlimit = 2
		t.NoError(s.Start(context.Background()))
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case height := <-s.Finished():
			t.Equal(to, height)
		case <-s.Done():
			t.NoError(s.Err())
		}
	})

	t.Run("fetch error; retry", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		var called int64

		s := NewSyncer(
			t.dummyNewBlockImporterFunc(),
			maps[0],
			nil,
			func(_ context.Context, height base.Height) (m base.BlockMap, found bool, _ error) {
				err := util.Retry(
					context.Background(),
					func() (bool, error) {
						index := (height - base.GenesisHeight).Int64()
						if index < 0 || index >= int64(len(maps)) {
							return false, nil
						}

						if index == 3 {
							atomic.AddInt64(&called, 1)
						}

						if index == 3 && atomic.LoadInt64(&called) < 3 {
							return true, isaac.ErrRetrySyncSources.Errorf("hehehe")
						}

						m = maps[index]
						found = true

						return false, nil
					},
					-1,
					time.Millisecond*10,
				)

				return m, found, err
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
			t.dummySetLastVoteproofs(),
			nil,
			nil,
			nil,
		)
		t.NoError(s.Start(context.Background()))
		defer s.Cancel()

		t.True(s.Add(to))

		select {
		case <-time.After(time.Second * 3):
			t.NoError(errors.Errorf("waits to be finished, but not"))
		case height := <-s.Finished():
			t.Equal(to, height)

			t.True(atomic.LoadInt64(&called) > 2)
		}
	})
}

func (t *testSyncer) TestFetchBlockItem() {
	lastheight := base.Height(3)
	to := lastheight + 10
	maps := t.maps(lastheight, to)

	lastvoteproofsavedch := make(chan struct{}, 1)
	s := NewSyncer(
		t.dummyNewBlockImporterFunc(),
		maps[0],
		nil,
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - lastheight).Int64()

			return maps[index], true, nil
		},
		t.dummyBlockMapItemFunc(),
		isaacdatabase.NewMemTempSyncPool(),
		func(isaac.BlockReader) error {
			lastvoteproofsavedch <- struct{}{}

			return nil
		},
		nil,
		nil,
		nil,
	)

	s.batchlimit = 2

	t.NoError(s.Start(context.Background()))
	defer s.Cancel()

	t.True(s.Add(to))

	select {
	case height := <-s.Finished():
		t.Equal(to, height)
	case <-s.Done():
		t.NoError(s.Err())
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("wait to set last voteproofs"))
	case <-lastvoteproofsavedch:
	}
}

func (t *testSyncer) TestFetchDifferentMap() {
	to := base.Height(3)
	maps := t.maps(base.GenesisHeight, to)

	prev := maps[2]
	t.T().Log("last block:", prev.Manifest().Height())

	diffprev, err := newTestBlockMap(
		prev.Manifest().Height(),
		maps[prev.Manifest().Height()-1].Manifest().Hash(),
		prev.Manifest().Suffrage(),
		t.Local, t.LocalParams.NetworkID(),
	)
	t.NoError(err)

	removeprevch := make(chan base.Height, 1)
	s := NewSyncer(
		t.dummyNewBlockImporterFunc(),
		diffprev,
		nil,
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		},
		t.dummyBlockMapItemFunc(),
		isaacdatabase.NewMemTempSyncPool(),
		t.dummySetLastVoteproofs(),
		nil,
		func(height base.Height) (bool, error) {
			if height == diffprev.Manifest().Height() {
				removeprevch <- height

				return true, nil
			}

			return false, nil
		},
		nil,
	)

	t.NoError(s.Start(context.Background()))
	defer s.Cancel()

	t.True(s.Add(to))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait previous block to be remove"))
	case height := <-removeprevch:
		t.Equal(diffprev.Manifest().Height(), height)
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait finished"))
	case height := <-s.Finished():
		t.Equal(to, height)
		t.NoError(s.Err())
	}
}

func (t *testSyncer) TestFetchDifferentMapFailedToRemovePrevious() {
	to := base.Height(3)
	maps := t.maps(base.GenesisHeight, to)

	prev := maps[2]
	t.T().Log("last block:", prev.Manifest().Height())

	diffprev, err := newTestBlockMap(
		prev.Manifest().Height(),
		maps[prev.Manifest().Height()-1].Manifest().Hash(),
		prev.Manifest().Suffrage(),
		t.Local, t.LocalParams.NetworkID(),
	)
	t.NoError(err)

	removeprevch := make(chan base.Height, 1)
	s := NewSyncer(
		nil,
		diffprev,
		nil,
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		},
		nil,
		isaacdatabase.NewMemTempSyncPool(),
		nil,
		nil,
		func(height base.Height) (bool, error) {
			if height == diffprev.Manifest().Height() {
				removeprevch <- height
			}

			return false, nil
		},
		nil,
	)
	t.NoError(s.Start(context.Background()))
	defer s.Cancel()

	t.True(s.Add(to))

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait previous block to be remove"))
	case height := <-removeprevch:
		t.Equal(diffprev.Manifest().Height(), height)
	}

	select {
	case <-time.After(time.Second * 2):
		t.NoError(errors.Errorf("failed to wait done"))
	case <-s.Done():
		t.Error(s.Err())
		t.ErrorContains(s.Err(), "previous manifest does not match with remotes")
	}
}

func TestSyncer(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSyncer))
}
