package isaacstates

import (
	"context"
	"io"
	"strings"
	"sync"
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
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignedFactHint, Instance: isaac.INITBallotSignedFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotSignedFactHint, Instance: isaac.ACCEPTBallotSignedFact{}}))
}

func (t *testSyncer) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testSyncer) TestNew() {
	s, err := NewSyncer(t.Root, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool())
	t.NoError(err)

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

func (t *testSyncer) dummyNewBlockImporterFunc() NewBlockImporterFunc {
	return func(string, base.Height) (isaac.BlockImporter, error) {
		return &isaacblock.DummyBlockImporter{}, nil
	}
}

func (t *testSyncer) dummyBlockMapItemFunc() SyncerBlockMapItemFunc {
	return func(context.Context, base.Height, base.BlockMapItemType) (io.Reader, bool, error) {
		return nil, true, nil
	}
}

func (t *testSyncer) TestAdd() {
	t.Run("with nil last", func() {
		s, err := NewSyncer(t.Root, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

		height := base.Height(33)
		t.True(s.Add(height))
		t.Equal(height, s.top())

		<-s.startsyncch
	})

	t.Run("same with last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s, err := NewSyncer(t.Root, nil, last, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

		height := base.Height(33)
		t.False(s.Add(height))
		t.Equal(lastheight, s.top())
	})

	t.Run("older than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s, err := NewSyncer(t.Root, nil, last, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

		height := lastheight - 1
		t.False(s.Add(height))
		t.Equal(lastheight, s.top())
	})

	t.Run("higher than last", func() {
		lastheight := base.Height(33)
		last := t.maps(lastheight, lastheight)[0]

		s, err := NewSyncer(t.Root, nil, last, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

		height := lastheight + 1
		t.True(s.Add(height))
		t.Equal(height, s.top())

		<-s.startsyncch
	})
}

func (t *testSyncer) TestAddChan() {
	t.Run("with nil last", func() {
		s, err := NewSyncer(t.Root, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

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

		s, err := NewSyncer(t.Root, nil, last, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

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
		s, err := NewSyncer(t.Root, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)

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
	s, err := NewSyncer(t.Root, nil, nil, nil, nil, isaacdatabase.NewMemTempSyncPool())
	t.NoError(err)
	t.NoError(s.Start())

	t.NoError(s.Cancel())
	t.NoError(s.Cancel())
}

func (t *testSyncer) TestFetchMaps() {
	t.Run("fetch error", func() {
		to := base.Height(5)
		maps := t.maps(base.GenesisHeight, to)

		s, err := NewSyncer(t.Root, nil, nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			if index == 1 {
				return nil, false, errors.Errorf("hehehe")
			}

			return maps[index], true, nil
		}, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)
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

		s, err := NewSyncer(t.Root, nil, nil, func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - base.GenesisHeight).Int64()
			if index < 0 || index >= int64(len(maps)) {
				return nil, false, nil
			}

			return maps[index], true, nil
		}, nil, isaacdatabase.NewMemTempSyncPool())
		t.NoError(err)
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

	t.Run("top updated", func() {
		to := base.Height(5)
		newto := to + 5
		maps := t.maps(base.GenesisHeight, newto)

		reachedlock := sync.NewCond(&sync.Mutex{})

		reachedch := make(chan struct{})
		var reached bool
		s, err := NewSyncer(
			t.Root,
			t.dummyNewBlockImporterFunc(),
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
		)
		t.NoError(err)
		t.NoError(s.Start())
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

		s, err := NewSyncer(
			t.Root,
			t.dummyNewBlockImporterFunc(),
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
		)
		t.NoError(err)
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

		s, err := NewSyncer(
			t.Root,
			t.dummyNewBlockImporterFunc(),
			maps[0],
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				index := (height - lastheight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
		)
		t.NoError(err)
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

		s, err := NewSyncer(
			t.Root,
			t.dummyNewBlockImporterFunc(),
			maps[0],
			func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
				index := (height - lastheight).Int64()
				if index < 0 || index >= int64(len(maps)) {
					return nil, false, nil
				}

				return maps[index], true, nil
			},
			t.dummyBlockMapItemFunc(),
			isaacdatabase.NewMemTempSyncPool(),
		)
		t.NoError(err)

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

// BLOCK test LeveldbTempSyncPool

func (t *testSyncer) TestFetchBlockItem() {
	lastheight := base.Height(3)
	to := lastheight + 10
	maps := t.maps(lastheight, to)

	s, err := NewSyncer(
		t.Root,
		t.dummyNewBlockImporterFunc(),
		maps[0],
		func(_ context.Context, height base.Height) (base.BlockMap, bool, error) {
			index := (height - lastheight).Int64()

			return maps[index], true, nil
		},
		t.dummyBlockMapItemFunc(),
		isaacdatabase.NewMemTempSyncPool(),
	)
	t.NoError(err)

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
}

func TestSyncer(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testSyncer))
}
