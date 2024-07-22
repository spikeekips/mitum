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

type testBlockItemStatesWriterNewWriteFunc struct {
	baseTestBaseBlockItemWriterNewWriteFunc
	baseTestWorkerMerger
}

func (t *testBlockItemStatesWriterNewWriteFunc) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newBlockItemStatesWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		_, err := c("")
		t.ErrorContains(err, "empty create id")
	})

	t.Run("zero total", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(0)(func(saveWorkerMergerSaveStateFunc) error {
			return nil
		})
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("zero total, but trying to write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(0)(func(w saveWorkerMergerSaveStateFunc) error {
			return w(t.randomState())
		})
		t.ErrorContains(err, "zero total")
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("single write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		pub := base.NewDummyPublickey("killme")
		st := base.NewBaseState(
			33,
			AccountStateKey(pub),
			NewAccountStateValue(pub, 99),
			nil,
			[]util.Hash{valuehash.NewBytes([]byte("findme"))},
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(1)(func(w saveWorkerMergerSaveStateFunc) error {
			return w(st)
		})
		t.NoError(err)
		t.NotNil(bm)

		t.Equal("143aadc8ae0048561ce8f999781127fef3752330a667e2350b134d30b7983d03", bm.Checksum())

		written, err := buf()
		t.NoError(err)
		t.T().Logf("written:\n%s", string(written))
		t.Equal("23207b22777269746572223a22626c6f636b2d6c6f63616c66732d7772697465722d3336342d2d76302e302e31222c22656e636f646572223a226a736f6e2d656e636f6465722d76302e302e31222c22746f74616c223a317d0a7b2268617368223a2233356663333231383133373663313564353363333163326465666163326363306661313937323462393031323135313035643635626665616162303632306639222c2270726576696f7573223a6e756c6c2c2276616c7565223a7b2261646472657373223a226b696c6c6d65647075222c225f68696e74223a22746573742d6163636f756e742d73746174652d76616c75652d76302e302e31222c2262616c616e6365223a39397d2c226b6579223a2274612d6b696c6c6d65647075222c226f7065726174696f6e73223a5b22363636393665363436643635225d2c225f68696e74223a22626173652d73746174652d76302e302e31222c22686569676874223a33337d0a", hex.EncodeToString(written))
	})

	t.Run("multiple write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemStatesWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		sts := make([]base.State, 33)
		for i := range sts {
			pub := base.NewDummyPublickey(util.UUID().String())

			sts[i] = t.newState(pub, int64(i+1))
		}

		f, err := c(util.UUID().String())
		t.NoError(err)

		jw, err := NewBaseJobWorker(context.Background(), int64(len(sts)))
		t.NoError(err)

		bm, err := f(uint64(len(sts)))(func(w saveWorkerMergerSaveStateFunc) error {
			for i := range sts {
				st := sts[i]

				if err := jw.NewJob(func(context.Context) error {
					return w(st)
				}); err != nil {
					return err
				}
			}

			jw.JobDone()

			return jw.Wait()

			return nil
		})
		t.NoError(err)
		t.NotNil(bm)

		t.NotEmpty(bm.Checksum())

		written, err := buf()
		t.NoError(err)
		t.T().Logf("written:\n%s", string(written))
		t.NotEmpty(written)
	})
}

func TestBlockItemStatesWriterNewWriteFunc(t *testing.T) {
	suite.Run(t, new(testBlockItemStatesWriterNewWriteFunc))
}
