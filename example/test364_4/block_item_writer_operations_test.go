package main

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/stretchr/testify/suite"
)

type testBlockItemOperationsWriterNewWriteFunc struct {
	baseTestBaseBlockItemWriterNewWriteFunc
}

func (t *testBlockItemOperationsWriterNewWriteFunc) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newBlockItemOperationsWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		_, err := c("")
		t.ErrorContains(err, "empty create id")
	})

	t.Run("zero total", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(0)(func(operationsWorkerSaveOperationFunc) error {
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

		c := newBlockItemOperationsWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(0)(func(w operationsWorkerSaveOperationFunc) error {
			return w(1, t.randomOp(3))
		})
		t.ErrorContains(err, "zero total")
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("single write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		ops := `{"hash":"54df80bfabc7d3784e73519febe257ec16305a53557a925202339df65e3f9bdd","fact":{"issuer":"showmmedpu","receiver":"findmemdpu","hash":"a6bbeecf1e183d844935f89ed77be17c58cd25d0a4bced2c1417397ba4afc865","token":"dG9rZW4=","_hint":"test-fact-v0.0.1","amount":33},"signs":[{"signed_at":"2024-07-22T06:25:03.831202297Z","signer":"showmmedpu","signature":"73686f776d6d65dbf2a1f0658d4e18b95c18e9bb3f5694a6bbeecf1e183d844935f89ed77be17c58cd25d0a4bced2c1417397ba4afc865323032342d30372d32322030363a32353a30332e383331202b3030303020555443"}],"_hint":"test-operation-v0.0.1"}`

		var op Operation
		t.NoError(encoder.Decode(t.enc, []byte(ops), &op))

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(1)(func(w operationsWorkerSaveOperationFunc) error {
			return w(3, op)
		})
		t.NoError(err)
		t.NotNil(bm)

		t.Equal("5e4b9d929c68f6ed8855a9d7b14155689920c6683a7ab9a17ab07ee588ab40ac", bm.Checksum())

		written, err := buf()
		t.NoError(err)
		t.T().Logf("written:\n%s", string(written))
		t.Equal("23207b22777269746572223a22626c6f636b2d6c6f63616c66732d7772697465722d3336342d76302e302e31222c22656e636f646572223a226a736f6e2d656e636f6465722d76302e302e31222c22746f74616c223a317d0a23206f70696e6465783d330a7b2268617368223a2235346466383062666162633764333738346537333531396665626532353765633136333035613533353537613932353230323333396466363565336639626464222c2266616374223a7b22697373756572223a2273686f776d6d65647075222c227265636569766572223a2266696e646d656d647075222c2268617368223a2261366262656563663165313833643834343933356638396564373762653137633538636432356430613462636564326331343137333937626134616663383635222c22746f6b656e223a22644739725a57343d222c225f68696e74223a22746573742d666163742d76302e302e31222c22616d6f756e74223a33337d2c227369676e73223a5b7b227369676e65645f6174223a22323032342d30372d32325430363a32353a30332e3833313230323239375a222c227369676e6572223a2273686f776d6d65647075222c227369676e6174757265223a223733363836663737366436643635646266326131663036353864346531386239356331386539626233663536393461366262656563663165313833643834343933356638396564373762653137633538636432356430613462636564326331343137333937626134616663383635333233303332333432643330333732643332333232303330333633613332333533613330333332653338333333313230326233303330333033303230353535343433227d5d2c225f68696e74223a22746573742d6f7065726174696f6e2d76302e302e31227d0a", hex.EncodeToString(written))
	})

	t.Run("multiple write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBlockItemOperationsWriterNewWriteFunc(
			t.enc,
			createWriter,
		)

		ops := make([]Operation, 3333)

		for i := range ops {
			ops[i] = t.randomOp(int64(i + 1))
		}

		f, err := c(util.UUID().String())
		t.NoError(err)

		jw, err := NewBaseJobWorker(context.Background(), int64(len(ops)))
		t.NoError(err)

		bm, err := f(uint64(len(ops)))(func(w operationsWorkerSaveOperationFunc) error {
			for i := range ops {
				opindex := uint64(i)

				if err := jw.NewJob(func(context.Context) error {
					return w(opindex, ops[opindex])
				}); err != nil {
					return err
				}
			}

			jw.JobDone()

			return jw.Wait()
		})
		t.NoError(err)
		t.NotNil(bm)

		t.NotEmpty(bm.Checksum())

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)
		t.T().Logf("written:\n%s", string(written[:999]))
	})
}

func TestBlockItemOperationsWriterNewWriteFunc(t *testing.T) {
	suite.Run(t, new(testBlockItemOperationsWriterNewWriteFunc))
}
