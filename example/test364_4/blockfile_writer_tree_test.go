package main

import (
	"context"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/stretchr/testify/suite"
)

type testBlockFileTreeWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testBlockFileTreeWriter) randomTree(n uint64) fixedtree.Tree {
	if n < 1 {
		return fixedtree.EmptyTree()
	}

	tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, n)
	if err != nil {
		panic(err)
	}

	for i := uint64(0); i < n; i++ {
		tw.Add(uint64(i), fixedtree.NewBaseNode(util.ULID().String()))
	}

	tr, err := tw.Tree()
	if err != nil {
		panic(err)
	}

	return tr
}

func (t *testBlockFileTreeWriter) newTree(keys ...string) fixedtree.Tree {
	tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, uint64(len(keys)))
	if err != nil {
		panic(err)
	}

	for i := range keys {
		tw.Add(uint64(i), fixedtree.NewBaseNode(keys[i]))
	}

	tr, err := tw.Tree()
	if err != nil {
		panic(err)
	}

	return tr
}

func (t *testBlockFileTreeWriter) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newFuncBlockFileTreeWriter(
			base.BlockItemOperationsTree,
			t.enc,
			createWriter,
			nil, nil,
		)

		_, err := c(context.Background(), "", t.randomTree(1))
		t.ErrorContains(err, "empty create id")
	})

	t.Run("empty tree", func() {
		createWriter, _ := t.newCreateWriter()

		c := newFuncBlockFileTreeWriter(
			base.BlockItemOperationsTree,
			t.enc,
			createWriter,
			nil, nil,
		)

		bms, err := c(context.Background(), util.UUID().String(), t.randomTree(0))
		t.NoError(err)
		t.Empty(bms)
	})

	t.Run("ok", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileTreeWriter(
			base.BlockItemOperationsTree,
			t.enc,
			createWriter,
			nil, nil,
		)

		bms, err := c(context.Background(), util.UUID().String(), t.newTree("a"))
		t.NoError(err)
		t.NotEmpty(bms)

		bufs(func(s [][]byte) error {
			for i := range s {
				written := s[i]

				t.T().Logf("written:\n%s", string(written))

				expected := `# {"tree":"operation-fixedtree-v0.0.1","writer":"block-localfs-writer-364-v0.0.1","encoder":"json-encoder-v0.0.1","total":1}
# index=0
{"hash":"80084bf2fba02475726feb2cab2d8215eab14bc6bdd8bfb2c8151257032ecd8b","key":"a"}
`
				t.Equal(expected, string(written))
			}

			return nil
		})
	})

	t.Run("multiple chunks", func() {
		createWriter, bufs := t.newCreateWriter()

		c := newFuncBlockFileTreeWriter(
			base.BlockItemOperationsTree,
			t.enc,
			createWriter,
			func() uint64 { return 33 },
			nil,
		)

		keys := make([]string, 3334)
		for i := range keys {
			keys[i] = util.ULID().String()
		}

		bms, err := c(context.Background(), util.UUID().String(), t.newTree(keys...))
		t.NoError(err)
		t.NotEmpty(bms)
		t.Equal(102, len(bms))

		bufs(func(s [][]byte) error {
			t.Equal(len(bms), len(s))

			for i := range s {
				t.NotEmpty(s[i])
			}

			return nil
		})
	})

	t.Run("context canceled", func() {
		createWriter, _ := t.newCreateWriter()

		c := newFuncBlockFileTreeWriter(
			base.BlockItemOperationsTree,
			t.enc,
			createWriter,
			nil, nil,
		)

		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		bms, err := c(ctx, util.UUID().String(), t.newTree("a"))
		t.ErrorIs(err, context.Canceled)
		t.Empty(bms)
	})
}

func TestBlockFileTreeWriter(t *testing.T) {
	suite.Run(t, new(testBlockFileTreeWriter))
}
