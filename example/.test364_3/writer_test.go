package main

import (
	"cmp"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testTreeWriter struct {
	isaacblock.BaseTestLocalBlockFS
}

func (t *testTreeWriter) resetFS(root string) {
	t.NoError(os.RemoveAll(root))
	t.NoError(os.Mkdir(root, 0o700))
}

func (t *testTreeWriter) printFS(root string) {
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.T().Logf("error: %+v", err)

			return err
		}

		if info.IsDir() {
			t.T().Log("directory:", path)

			return nil
		}

		t.T().Log("file:", path)

		f, err := os.Open(path)
		t.NoError(err)
		var b []byte
		if strings.HasSuffix(path, ".gz") {
			gr, err := gzip.NewReader(f)
			t.NoError(err)
			i, err := io.ReadAll(gr)
			t.NoError(err)

			b = i
		} else {
			i, err := io.ReadAll(f)
			t.NoError(err)

			b = i
		}
		t.T().Logf(`-x--------------------------------------------
%s
---------------------------------------------x-`, string(b))

		return nil
	})
}

func (t *testTreeWriter) Run(name string, f func()) {
	defer t.resetFS(t.Root)
	defer t.printFS(t.Root)

	t.Suite.Run(name, f)
}

func (t *testTreeWriter) saveNodeFuncs(name string) (
	string,
	func(uint64, fixedtree.Node) error,
	func() error,
	error,
) {
	fpath := filepath.Join(t.Root, name)

	newFile := func() (io.WriteCloser, error) {
		f, err := os.OpenFile(fpath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}

		g, err := util.NewGzipWriter(f, gzip.BestSpeed)
		if err != nil {
			_ = f.Close()

			return nil, err
		}

		return g, nil
	}

	var g io.WriteCloser
	var once sync.Once

	return fpath,
		func(index uint64, node fixedtree.Node) error {
			var gerr error

			once.Do(func() {
				switch i, err := newFile(); {
				case err != nil:
					gerr = err
				default:
					g = i
				}
			})

			if gerr != nil {
				return gerr
			}

			switch b, err := t.Enc.Marshal(node); {
			case err != nil:
				return err
			default:
				if _, err := g.Write(
					util.ConcatBytesSlice(
						[]byte(fmt.Sprintf("%d,", index)),
						b,
						[]byte("\n"),
					),
				); err != nil {
					return err
				}

				return nil
			}
		},
		func() error {
			if g == nil {
				return nil
			}

			return g.Close()
		},
		nil
}

func (t *testTreeWriter) TestCancel() {
	t.Run("parent ctx canceled", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, saveNodeFunc, closef, err := t.saveNodeFuncs("states_tree.gz")
		t.NoError(err)
		defer closef()

		closesavech := make(chan bool, 1)

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(ctx context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(index uint64, node fixedtree.Node) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(time.Second * 3):
							return saveNodeFunc(index, node)
						}
					},
					func(ok bool) (string, error) {
						closesavech <- ok

						return "", closef()
					},
					nil
			},
			1,
		)
		t.NoError(err)

		for i := 0; i < 33; i++ {
			t.NoError(w.Add(util.UUID().String(), fixedtree.NewBaseNode(valuehash.RandomSHA256().String())))
		}

		cancel()

		h, err := w.Tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(h)

		_, err = w.Save()
		t.ErrorIs(err, context.Canceled)
		t.Nil(h)

		select {
		case <-time.After(time.Second):
			t.Fail("wait close save")
		case ok := <-closesavech:
			t.False(ok)
		}
	})

	t.Run("Cancel()", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		_, saveNodeFunc, closef, err := t.saveNodeFuncs("states_tree.gz")
		t.NoError(err)
		defer closef()

		closesavech := make(chan bool, 1)

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(ctx context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(index uint64, node fixedtree.Node) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(time.Second * 3):
							return saveNodeFunc(index, node)
						}
					},
					func(ok bool) (string, error) {
						closesavech <- ok

						return "", closef()
					},
					nil
			},
			1,
		)
		t.NoError(err)

		for i := 0; i < 33; i++ {
			t.NoError(w.Add(util.UUID().String(), fixedtree.NewBaseNode(valuehash.RandomSHA256().String())))
		}

		w.Cancel()

		h, err := w.Tree()
		t.ErrorIs(err, context.Canceled)
		t.Nil(h)

		_, err = w.Save()
		t.ErrorIs(err, context.Canceled)
		t.Nil(h)

		select {
		case <-time.After(time.Second):
			t.Fail("wait close save")
		case ok := <-closesavech:
			t.False(ok)
		}
	})

	t.Run("cancel before add", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(uint64, fixedtree.Node) error { return nil },
					func(bool) (string, error) { return "", nil },
					nil
			},
			1,
		)
		t.NoError(err)

		w.Cancel()

		_, err = w.Tree()
		t.ErrorIs(err, context.Canceled)

		_, err = w.Save()
		t.ErrorIs(err, context.Canceled)
	})

	t.Run("cancel before add done", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		closesavech := make(chan bool, 1)

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(uint64, fixedtree.Node) error { return nil },
					func(ok bool) (string, error) {
						closesavech <- ok

						return "", nil
					},
					nil
			},
			1,
		)
		t.NoError(err)

		t.NoError(w.Add(util.UUID().String(), fixedtree.NewBaseNode(valuehash.RandomSHA256().String())))

		w.Cancel()

		_, err = w.Tree()
		t.ErrorIs(err, context.Canceled)

		_, err = w.Save()
		t.ErrorIs(err, context.Canceled)

		select {
		case <-time.After(time.Second):
			t.Fail("wait close save")
		case ok := <-closesavech:
			t.False(ok)
		}
	})

	t.Run("save error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		closeokch := make(chan bool, 1)

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(ctx context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(index uint64, node fixedtree.Node) error {
						if index < 1 {
							return nil
						}

						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-time.After(time.Millisecond * 300):
							return errors.Errorf("showme")
						}
					},
					func(ok bool) (string, error) {
						closeokch <- ok
						return "", nil
					},
					nil
			},
			1,
		)
		t.NoError(err)

		for i := 0; i < 3; i++ {
			t.NoError(w.Add(util.UUID().String(), fixedtree.NewBaseNode(valuehash.RandomSHA256().String())))
		}

		h, err := w.Tree()
		t.NoError(err)
		t.NotNil(h)

		_, err = w.Save()
		t.ErrorContains(err, "showme")

		t.False(<-closeokch)
	})
}

func (t *testTreeWriter) TestStatesTree() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		savedFile, saveNodeFunc, closef, err := t.saveNodeFuncs("states_tree.gz")
		t.NoError(err)
		defer closef()

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(index uint64, node fixedtree.Node) error {
						return saveNodeFunc(index, node)
					},
					func(bool) (string, error) { return "", closef() },
					nil
			},
			3,
		)
		t.NoError(err)

		nodes := make([]fixedtree.Node, 33)
		for i := range nodes {
			h := valuehash.RandomSHA256()

			node := newOrderedTreeNode(
				util.UUID().String(),
				fixedtree.NewBaseNode(h.String()),
			)
			nodes[i] = node

			t.NoError(w.Add(node.K, fixedtree.NewBaseNode(h.String())))
		}

		slices.SortFunc(nodes, func(a, b fixedtree.Node) int {
			return cmp.Compare(
				a.(orderedTreeNode[string]).K,
				b.(orderedTreeNode[string]).K,
			)
		})

		tg, err := fixedtree.NewWriterFromNodes(base.StateFixedtreeHint, nodes)
		t.NoError(err)
		tr, err := tg.Tree()
		t.NoError(err)

		h, err := w.Tree()
		t.NoError(err)
		t.NotNil(h)

		_, err = w.Save()
		t.NoError(err)

		rtr, err := LoadTreeFromFile(t.Enc, base.StateFixedtreeHint, savedFile)
		t.NoError(err)

		t.Equal(tr.Len(), rtr.Len())

		rtr.Traverse(func(index uint64, b fixedtree.Node) (bool, error) {
			t.True(b.Equal(tr.Node(index)))

			return true, nil
		})

		fi, err := os.Stat(savedFile)
		t.NoError(err)
		t.False(fi.IsDir())
	})

	t.Run("empty", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		savedFile, saveNodeFunc, closef, err := t.saveNodeFuncs("states_tree.gz")
		t.NoError(err)
		defer closef()

		closesavech := make(chan bool, 1)

		w, err := NewBaseFixedTreeWriter[string](
			ctx,
			func(ctx context.Context) (JobWorker, error) {
				return NewBaseJobWorker(ctx, 1<<9)
			},
			base.StateFixedtreeHint,
			func(context.Context) (writerSaveTreeNodeFunc, writerCloseSaveFunc, error) {
				return func(index uint64, node fixedtree.Node) error {
						return saveNodeFunc(index, node)
					},
					func(ok bool) (string, error) {
						closesavech <- ok

						return "", closef()
					},
					nil
			},
			3,
		)
		t.NoError(err)

		h, err := w.Tree()
		t.NoError(err)
		t.Nil(h)

		_, err = w.Save()
		t.NoError(err)

		_, err = os.Stat(savedFile)
		t.True(os.IsNotExist(err))

		select {
		case <-time.After(time.Second):
			t.Fail("wait close save")
		case ok := <-closesavech:
			t.True(ok)
		}
	})
}

func TestTreeWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testTreeWriter))
}
