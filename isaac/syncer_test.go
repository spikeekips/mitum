package isaac

import (
	"context"
	"testing"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type dummyNodeConnInfo struct {
	quicmemberlist.NamedConnInfo
	base.BaseNode
}

func newDummyNodeConnInfo(address base.Address, pub base.Publickey, addr string, tlsinsecure bool) dummyNodeConnInfo {
	return dummyNodeConnInfo{
		BaseNode:      NewNode(pub, address),
		NamedConnInfo: quicmemberlist.NewNamedConnInfo(addr, tlsinsecure),
	}
}

func (n dummyNodeConnInfo) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid dummyNodeConnInfo")

	if err := n.BaseNode.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := n.NamedConnInfo.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

type testSyncSourcePool struct {
	suite.Suite
}

func (t *testSyncSourcePool) newnci() dummyNodeConnInfo {
	ci := quicstream.RandomConnInfo()

	return newDummyNodeConnInfo(
		base.RandomAddress(""),
		base.NewMPrivatekey().Publickey(),
		ci.Addr().String(),
		ci.TLSInsecure(),
	)
}

func (t *testSyncSourcePool) TestNew() {
	t.Run("ok", func() {
		sources := make([]NodeConnInfo, 3)

		for i := range sources {
			sources[i] = t.newnci()
		}

		p := NewSyncSourcePool(sources)

		t.NotEmpty(p.sourceids)
		t.Equal(0, p.index)
	})

	t.Run("empty", func() {
		p := NewSyncSourcePool(nil)

		t.Empty(p.sourceids)
		t.Equal(0, p.index)
	})
}

func (t *testSyncSourcePool) TestUpdate() {
	prevsources := make([]NodeConnInfo, 3)
	newsources := make([]NodeConnInfo, 3)

	for i := range prevsources {
		prevsources[i] = t.newnci()
		newsources[i] = t.newnci()
	}

	t.Run("update", func() {
		p := NewSyncSourcePool(prevsources)
		previds := p.sourceids

		t.True(p.UpdateFixed(newsources))
		t.False(p.UpdateFixed(newsources))
		t.True(p.UpdateFixed(prevsources))

		t.Equal(previds, p.sourceids)
	})

	t.Run("update and reset", func() {
		p := NewSyncSourcePool(prevsources)

		t.True(p.UpdateFixed(newsources))

		_, previd, err := p.Next("")
		t.NoError(err)
		t.Equal(p.sourceids[0], previd)

		t.True(p.UpdateFixed(prevsources))

		_, nextid, err := p.Next(previd)
		t.NoError(err)
		t.Equal(p.sourceids[0], nextid)
	})

	t.Run("update empty", func() {
		p := NewSyncSourcePool(prevsources)

		t.True(p.UpdateFixed(newsources))

		_, previd, err := p.Next("")
		t.NoError(err)
		t.Equal(p.sourceids[0], previd)

		t.True(p.UpdateFixed(nil))

		_, _, err = p.Next(previd)
		t.Error(err)
		t.True(errors.Is(err, ErrEmptySyncSources))
	})
}

func (t *testSyncSourcePool) TestAdd() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	t.Run("new", func() {
		p := NewSyncSourcePool(sources)

		prev := p.sources
		previds := p.sourceids

		added := t.newnci()
		t.True(p.Add(added))

		next := make([]NodeConnInfo, len(prev)+1)
		copy(next, prev)
		next[len(prev)] = added
		nextids := make([]string, len(prev)+1)
		copy(nextids, previds)
		nextids[len(previds)] = p.makesourceid(added)

		t.Equal(p.sources[len(p.sources)-1].String(), added.String())
		t.Equal(nextids, p.sourceids)
	})

	t.Run("known", func() {
		p := NewSyncSourcePool(sources)

		prev := p.sources
		previds := p.sourceids

		t.False(p.Add(sources[1]))

		t.Equal(len(p.sources), len(prev))
		t.Equal(p.sources[len(p.sources)-1].String(), prev[len(prev)-1].String())
		t.Equal(previds, p.sourceids)
	})

	t.Run("update", func() {
		p := NewSyncSourcePool(sources)

		prev := p.sources

		newci := quicstream.RandomConnInfo()

		added := sources[1]
		added = newDummyNodeConnInfo(
			added.Address(),
			added.Publickey(),
			newci.Addr().String(),
			newci.TLSInsecure(),
		)

		t.True(p.Add(added))

		t.Equal(len(p.sources), len(prev))

		t.Equal(p.sources[1].String(), added.String())
		t.Equal(p.sourceids[1], p.makesourceid(added))
	})

	t.Run("next and update", func() {
		p := NewSyncSourcePool(sources)

		_, previd, err := p.Next("")
		t.NoError(err)
		_, nextid, err := p.Next(previd)
		t.NoError(err)
		t.NotEqual(previd, nextid)

		prev := p.sources

		newci := quicstream.RandomConnInfo()

		added := sources[1]
		added = newDummyNodeConnInfo(
			added.Address(),
			added.Publickey(),
			newci.Addr().String(),
			newci.TLSInsecure(),
		)

		t.True(p.Add(added))

		t.Equal(len(p.sources), len(prev))

		t.Equal(p.sources[1].String(), added.String())
		t.Equal(p.sourceids[1], p.makesourceid(added))

		_, id, err := p.Next("")
		t.NoError(err)

		t.Equal(p.sourceids[0], id)
	})
}

func (t *testSyncSourcePool) TestRemove() {
	sources := make([]NodeConnInfo, 3)
	for i := range sources {
		sources[i] = t.newnci()
	}

	t.Run("ok", func() {
		p := NewSyncSourcePool(sources)

		added := make([]NodeConnInfo, 3)
		for i := range added {
			added[i] = t.newnci()
		}

		t.True(p.Add(added...))

		prev := p.sources
		previds := p.sourceids

		i := added[0]
		t.True(p.Remove(i.Address(), i.String()))

		next := make([]NodeConnInfo, len(prev)-1)
		copy(next, prev[:3])
		copy(next[3:], prev[4:])

		nextids := make([]string, len(prev)-1)
		copy(nextids, previds[:3])
		copy(nextids[3:], previds[4:])

		t.Equal(len(prev)-1, len(p.sources))
		t.Equal(nextids, p.sourceids)
	})

	t.Run("known but in fixed", func() {
		p := NewSyncSourcePool(sources)

		prev := p.sources
		previds := p.sourceids
		prevfixedlen := p.fixedlen

		i := p.sources[1]
		t.True(p.Remove(i.Address(), i.String()))

		next := make([]NodeConnInfo, len(prev)-1)
		copy(next, prev[:1])
		copy(next[1:], prev[2:])

		nextids := make([]string, len(prev)-1)
		copy(nextids, previds[:1])
		copy(nextids[1:], previds[2:])

		t.Equal(len(prev)-1, len(p.sources))
		t.Equal(nextids, p.sourceids)
		t.Equal(prevfixedlen-1, p.fixedlen)
	})

	t.Run("ok, next and reset", func() {
		p := NewSyncSourcePool(sources)

		added := make([]NodeConnInfo, 3)
		for i := range added {
			added[i] = t.newnci()
		}

		t.True(p.Add(added...))

		_, previd, err := p.Next("")
		t.NoError(err)
		_, previd, err = p.Next(previd)
		t.NoError(err)
		_, previd, err = p.Next(previd)
		t.NoError(err)
		_, previd, err = p.Next(previd)
		t.NoError(err)

		i := added[0]
		t.True(p.Remove(i.Address(), i.String()))

		_, previd, err = p.Next(previd)
		t.NoError(err)

		t.Equal(p.sourceids[2], previd)
	})
}

func (t *testSyncSourcePool) TestSameID() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	p0 := NewSyncSourcePool(sources)
	p1 := NewSyncSourcePool(sources)

	t.Equal(p0.sourceids, p1.sourceids)
}

func (t *testSyncSourcePool) TestNext() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	p := NewSyncSourcePool(sources)

	u := make([]NodeConnInfo, len(sources)*2)

	var id string
	for i := range make([]struct{}, len(sources)*2) {
		s, j, err := p.Next(id)
		t.NoError(err)

		u[i] = s
		id = j
	}

	for i := range u {
		a := sources[i%len(sources)]
		b := u[i]

		t.True(a.Address().Equal(b.Address()))
		t.True(a.Publickey().Equal(b.Publickey()))
		t.Equal(a.String(), b.String())
	}
}

func (t *testSyncSourcePool) TestNextPrevID() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	t.Run("empty id", func() {
		p := NewSyncSourcePool(sources)

		next, id, err := p.Next("")
		t.NoError(err)
		t.NotEmpty(id)
		t.NotNil(next)
		t.Equal(p.currentid, id)

		a := sources[0]

		t.True(a.Address().Equal(next.Address()))
		t.True(a.Publickey().Equal(next.Publickey()))
		t.Equal(a.String(), next.String())
	})

	t.Run("empty id and next", func() {
		p := NewSyncSourcePool(sources)

		next, id, err := p.Next("")
		t.NoError(err)
		t.NotEmpty(id)
		t.NotNil(next)
		t.Equal(p.currentid, id)

		next, _, err = p.Next(id)
		t.NoError(err)

		a := sources[1]

		t.True(a.Address().Equal(next.Address()))
		t.True(a.Publickey().Equal(next.Publickey()))
		t.Equal(a.String(), next.String())
	})

	t.Run("same id", func() {
		p := NewSyncSourcePool(sources)

		_, id, _ := p.Next("")
		t.Equal(p.currentid, id)

		next0, _, _ := p.Next(id)
		next1, _, _ := p.Next(id)

		t.True(next0.Address().Equal(next1.Address()))
		t.True(next0.Publickey().Equal(next1.Publickey()))
		t.Equal(next0.String(), next1.String())
	})
}

func (t *testSyncSourcePool) TestNextButEmpty() {
	p := NewSyncSourcePool(nil)

	next, id, err := p.Next("")
	t.Error(err)
	t.True(errors.Is(err, ErrEmptySyncSources))
	t.Nil(next)
	t.Empty(id)
}

func (t *testSyncSourcePool) TestConcurrent() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	p := NewSyncSourcePool(sources)

	t.NoError(util.RunErrgroupWorker(context.Background(), 333, func(_ context.Context, i uint64, _ uint64) error {
		previd := p.sourceids[i%uint64(len(p.sourceids))]
		if i%15 == 0 {
			previd = ""
		}

		next, id, err := p.Next(previd)

		switch {
		case err != nil:
			return err
		case len(id) < 1:
			return errors.Errorf("empty id")
		case next == nil:
			return errors.Errorf("empty next")
		default:
			return nil
		}
	}))
}

func (t *testSyncSourcePool) TestRetry() {
	sources := make([]NodeConnInfo, 3)

	for i := range sources {
		sources[i] = t.newnci()
	}

	t.Run("once", func() {
		p := NewSyncSourcePool(sources)

		var called int
		err := p.Retry(context.Background(), func(ci NodeConnInfo) (bool, error) {
			called++

			return false, nil
		}, 3, time.Millisecond*10)
		t.NoError(err)

		t.Equal(1, called)
	})

	t.Run("error once", func() {
		p := NewSyncSourcePool(sources)

		var called int
		err := p.Retry(context.Background(), func(ci NodeConnInfo) (bool, error) {
			called++

			if ci.Address().Equal(sources[0].Address()) {
				return false, errors.Errorf("hihihi")
			}

			return false, nil
		}, 3, time.Millisecond*10)
		t.Error(err)
		t.ErrorContains(err, "hihihi")

		t.Equal(1, called)
	})

	t.Run("ErrRetrySyncSources once", func() {
		p := NewSyncSourcePool(sources)

		var called int
		var last NodeConnInfo
		err := p.Retry(context.Background(), func(ci NodeConnInfo) (bool, error) {
			called++

			last = ci

			if ci.Address().Equal(sources[0].Address()) {
				return false, ErrRetrySyncSources.Errorf("hihihi")
			}

			return false, nil
		}, 3, time.Millisecond*10)
		t.NoError(err)

		t.Equal(2, called)

		next := sources[1]

		t.True(last.Address().Equal(next.Address()))
		t.True(last.Publickey().Equal(next.Publickey()))
		t.Equal(last.String(), next.String())
	})

	t.Run("network error once", func() {
		p := NewSyncSourcePool(sources)

		var called int
		var last NodeConnInfo
		err := p.Retry(context.Background(), func(ci NodeConnInfo) (bool, error) {
			called++

			last = ci

			if ci.Address().Equal(sources[0].Address()) {
				return false, &quic.StreamError{StreamID: 333, ErrorCode: quic.StreamErrorCode(444)}
			}

			return false, nil
		}, 3, time.Millisecond*10)
		t.NoError(err)

		t.Equal(2, called)

		next := sources[1]

		t.True(last.Address().Equal(next.Address()))
		t.True(last.Publickey().Equal(next.Publickey()))
		t.Equal(last.String(), next.String())
	})

	t.Run("long endure error", func() {
		p := NewSyncSourcePool(sources)

		var called int

		err := p.Retry(context.Background(), func(ci NodeConnInfo) (bool, error) {
			called++

			if called > 2 && ci.Address().Equal(sources[1].Address()) {
				return false, errors.Errorf("hihihi")
			}

			return false, &quic.StreamError{StreamID: 333, ErrorCode: quic.StreamErrorCode(444)}
		}, -1, time.Millisecond*10)
		t.Error(err)
		t.ErrorContains(err, "hihihi")

		t.Equal(len(sources)+2, called)
	})
}

func TestSyncSourcePool(tt *testing.T) {
	suite.Run(tt, new(testSyncSourcePool))
}
