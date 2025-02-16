package main

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBlockFileStatesWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testBlockFileStatesWriter) TestWrite() {
	ctx := context.Background()

	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			nil, nil,
		)

		_, err := c(ctx, "", 1, nil)
		t.ErrorContains(err, "empty create id")
	})

	t.Run("zero total", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			nil, nil,
		)

		bms, err := c(ctx, util.UUID().String(), 0, func(workerSaveStateFunc) error {
			return nil
		})
		t.NoError(err)
		t.Empty(bms)

		bufs(func(s [][]byte) error {
			t.Empty(s)

			return nil
		})
	})

	t.Run("zero total, but trying to write", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			nil, nil,
		)

		bms, err := c(ctx, util.UUID().String(), 0, func(w workerSaveStateFunc) error {
			return w(t.randomState())
		})
		t.NoError(err)
		t.Empty(bms)

		bufs(func(s [][]byte) error {
			t.Empty(s)

			return nil
		})
	})

	t.Run("single write", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			nil, nil,
		)

		pub := base.NewDummyPublickey("killme")
		st := base.NewBaseState(
			33,
			AccountStateKey(pub),
			NewAccountStateValue(pub, 99),
			nil,
			[]util.Hash{valuehash.NewBytes([]byte("findme"))},
		)

		bms, err := c(ctx, util.UUID().String(), 1, func(w workerSaveStateFunc) error {
			return w(st)
		})
		t.NoError(err)
		t.NotEmpty(bms)

		t.Equal("c24ceb681c336109495913fc6ff987e16f3e8be1fe1c441095df2c2dbf8dcf6c", bms[0].Checksum())

		bufs(func(s [][]byte) error {
			t.Equal(1, len(s))

			written := s[0]

			t.T().Logf("written:\n%s", string(written))
			t.Equal("23207b22777269746572223a22626c6f636b2d6c6f63616c66732d7772697465722d3336342d76302e302e31222c22656e636f646572223a226a736f6e2d656e636f6465722d76302e302e31222c22746f74616c223a317d0a232073746174655f6b65793d74612d6b696c6c6d656470750a7b2268617368223a2233356663333231383133373663313564353363333163326465666163326363306661313937323462393031323135313035643635626665616162303632306639222c2270726576696f7573223a6e756c6c2c2276616c7565223a7b2261646472657373223a226b696c6c6d65647075222c225f68696e74223a22746573742d6163636f756e742d73746174652d76616c75652d76302e302e31222c2262616c616e6365223a39397d2c226b6579223a2274612d6b696c6c6d65647075222c226f7065726174696f6e73223a5b22363636393665363436643635225d2c225f68696e74223a22626173652d73746174652d76302e302e31222c22686569676874223a33337d0a", hex.EncodeToString(written))

			return nil
		})
	})

	t.Run("multiple write", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			nil, nil,
		)

		sts := make([]base.State, 3333)
		for i := range sts {
			pub := base.NewDummyPublickey(util.UUID().String())

			sts[i] = t.newState(pub, int64(i+1))
		}

		bms, err := c(ctx, util.UUID().String(), uint64(len(sts)), func(w workerSaveStateFunc) error {
			for i := range sts {
				return w(sts[i])
			}

			return nil
		})
		t.NoError(err)
		t.NotEmpty(bms)

		t.NotEmpty(bms[0].Checksum())

		bufs(func(s [][]byte) error {
			t.Equal(1, len(s))

			written := s[0]

			t.T().Logf("written:\n%s", string(written[:len(written)%999]))
			t.NotEmpty(written)

			return nil
		})
	})

	t.Run("multiple chunks write", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileStatesWriter(
			t.enc,
			createWriter,
			func() uint64 { return 33 },
			nil,
		)

		sts := make([]base.State, 3333)
		for i := range sts {
			pub := base.NewDummyPublickey(util.UUID().String())

			sts[i] = t.newState(pub, int64(i+1))
		}

		bms, err := c(ctx, util.UUID().String(), uint64(len(sts)), func(w workerSaveStateFunc) error {
			for i := range sts {
				return w(sts[i])
			}

			return nil
		})
		t.NoError(err)
		t.NotEmpty(bms)

		t.NotEmpty(bms[0].Checksum())

		bufs(func(s [][]byte) error {
			for i := range s {
				t.NotEmpty(bms[i].Checksum())

				written := s[i]

				t.NotEmpty(written)
			}

			return nil
		})
	})
}

func TestBlockFileStatesWriter(t *testing.T) {
	suite.Run(t, new(testBlockFileStatesWriter))
}
