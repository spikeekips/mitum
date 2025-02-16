package main

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testBlockFileOperationsTreeWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testBlockFileOperationsTreeWriter) TestWrite() {
	t.Run("ok", func() {
		tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, 1)
		if err != nil {
			panic(err)
		}

		t.NoError(tw.Add(0, base.NewInStateOperationFixedtreeNode(valuehash.NewBytes([]byte("showme")), "")))

		tr, err := tw.Tree()
		t.NoError(err)

		createWriter, bufs := t.newCreateWriter()
		c := newFuncBlockFileOperationsTreeWriter(t.enc, createWriter, nil, nil)

		bms, err := c(context.Background(), util.UUID().String(), tr)
		t.NoError(err)
		t.NotEmpty(bms)

		bufs(func(s [][]byte) error {
			t.Equal(len(bms), len(s))

			for i := range s {
				written := s[i]

				t.T().Logf("written %d:\n%s", i, string(written))

				expected := `# {"tree":"operation-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"reason":null,"hash":"b05b88b7511e113372d1637f2d2cdd0f4448efcb8b5bee99120023964786b66a","key":"73686f776d65"}
`
				t.Equal(expected, string(written))
			}

			return nil
		})
	})

	t.Run("wrong tree hint", func() {
		tw, err := fixedtree.NewWriter(base.StateFixedtreeHint, 1)
		if err != nil {
			panic(err)
		}

		t.NoError(tw.Add(0, base.NewInStateOperationFixedtreeNode(valuehash.NewBytes([]byte("showme")), "")))

		tr, err := tw.Tree()
		t.NoError(err)

		createWriter, _ := t.newCreateWriter()
		c := newFuncBlockFileOperationsTreeWriter(t.enc, createWriter, nil, nil)

		bms, err := c(context.Background(), util.UUID().String(), tr)
		t.ErrorIs(err, util.ErrInvalid)
		t.Empty(bms)
	})
}

func TestBlockFileOperationsTreeWriter(t *testing.T) {
	suite.Run(t, new(testBlockFileOperationsTreeWriter))
}
