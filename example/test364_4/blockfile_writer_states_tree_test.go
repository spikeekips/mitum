package main

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
)

type testBlockFileStatesTreeWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testBlockFileStatesTreeWriter) TestWrite() {
	t.Run("ok", func() {
		tw, err := fixedtree.NewWriter(base.StateFixedtreeHint, 1)
		if err != nil {
			panic(err)
		}

		t.NoError(tw.Add(0, fixedtree.NewBaseNode("a")))

		tr, err := tw.Tree()
		t.NoError(err)

		createWriter, bufs := t.newCreateWriter()
		c := newFuncBlockFileStatesTreeWriter(t.enc, createWriter, nil, nil)

		bms, err := c(context.Background(), util.UUID().String(), tr)
		t.NoError(err)
		t.NotEmpty(bms)
		t.Equal(1, len(bms))

		bufs(func(s [][]byte) error {
			t.Equal(len(bms), len(s))

			written := s[0]

			t.T().Logf("written:\n%s", string(written))

			expected := `# {"tree":"state-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"hash":"80084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b","key":"a"}
`
			t.Equal(expected, string(written))

			return nil
		})
	})

	t.Run("wrong tree hint", func() {
		tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, 1)
		if err != nil {
			panic(err)
		}

		t.NoError(tw.Add(0, fixedtree.NewBaseNode(util.UUID().String())))

		tr, err := tw.Tree()
		t.NoError(err)

		createWriter, _ := t.newCreateWriter()
		c := newFuncBlockFileStatesTreeWriter(t.enc, createWriter, nil, nil)

		bms, err := c(context.Background(), util.UUID().String(), tr)
		t.ErrorIs(err, util.ErrInvalid)
		t.Empty(bms)
	})
}

func TestBlockFileStatesTreeWriter(t *testing.T) {
	suite.Run(t, new(testBlockFileStatesTreeWriter))
}
