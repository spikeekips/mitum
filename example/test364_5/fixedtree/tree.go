package fixedtree

import (
	"context"
	"iter"
	"math"
	"slices"

	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type Tree struct {
	nodes []Node
	hint.BaseHinter
}

func NewTree(ht hint.Hint, nodes []Node) *Tree {
	return &Tree{
		BaseHinter: hint.NewBaseHinter(ht),
		nodes:      nodes,
	}
}

func (t *Tree) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("Tree")

	if err := t.BaseHinter.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return e.Wrap(AsyncIsValidNodesHash(
		context.Background(), uint64(len(t.nodes)), b, t.nodes))
}

func (t *Tree) Close() {
	dutil.ClearSlice(t.nodes)
}

func (t *Tree) Len() int {
	return len(t.nodes)
}

func (t *Tree) Nodes() []Node {
	return t.nodes
}

func (t *Tree) Node(index uint64) Node {
	if index >= uint64(t.Len()) {
		return nil
	}

	return t.nodes[index]
}

func (t *Tree) Root() util.Hash {
	switch {
	case len(t.nodes) < 1:
		return nil
	case t.nodes[0] == nil:
		return nil
	default:
		return t.nodes[0].Hash()
	}
}

func (t *Tree) Traverse() iter.Seq2[uint64, Node] {
	return func(yield func(uint64, Node) bool) {
		if yield == nil {
			return
		}

		for i, n := range slices.All(t.nodes) {
			if !yield(uint64(i), n) {
				return
			}
		}
	}
}

func childrenNodes(index uint64, nodes []Node) (c [2]Node, err error) {
	i, err := children(uint64(len(nodes)), index)
	if err != nil {
		return c, err
	}

	switch {
	case i[0] >= uint64(len(nodes)):
	default:
		c[0] = nodes[i[0]]
	}

	switch {
	case i[1] >= uint64(len(nodes)):
	default:
		c[1] = nodes[i[1]]
	}

	return c, nil
}

func indexHeight(index uint64) uint64 {
	if index == 0 {
		return 0
	}

	return uint64(math.Log(float64(index+1)) / math.Log(2))
}

func lenNodesInHeight(size, height uint64) uint64 {
	cf := uint64(math.Pow(2, float64(height)) - 1)

	switch l := uint64(math.Pow(2, float64(height))); {
	case cf+l > size:
		return size - cf
	default:
		return l
	}
}

func indicesInHeight(size, height uint64) iter.Seq[uint64] {
	cf := uint64(math.Pow(2, float64(height)) - 1)

	var n uint64

	switch l := uint64(math.Pow(2, float64(height))); {
	case cf+l > size:
		n = size - cf
	default:
		n = l
	}

	return func(yield func(uint64) bool) {
		if yield == nil {
			return
		}

		for i := range n {
			if !yield(cf + uint64(i)) {
				return
			}
		}
	}
}

func children(size, index uint64) (c [2]uint64, err error) {
	height := indexHeight(index)

	currentFirst := uint64(math.Pow(2, float64(height)) - 1)
	nextFirst := uint64(math.Pow(2, float64(height+1)) - 1)
	pos := index - currentFirst

	switch i := nextFirst + pos*2; { //nolint:mnd //...
	case i >= uint64(size):
		return c, errNoChildren.WithStack()
	default:
		c[0] = i
	}

	c[1] = nextFirst + pos*2 + 1

	return c, nil
}

func parent(index uint64) (uint64, error) {
	height := indexHeight(index)
	if height == 0 {
		return 0, errNoParent.WithStack()
	}

	currentFirst := uint64(math.Pow(2, float64(height)) - 1)
	pos := index - currentFirst

	if pos%2 == 1 {
		pos--
	}

	upFirst := uint64(math.Pow(2, float64(height-1)) - 1)

	return upFirst + pos/2, nil
}

func baseTraverseByHeight(
	size uint64,
	h func(uint64, uint64) uint64,
) (iter.Seq2[uint64 /* height */, iter.Seq[uint64] /* nodes */], error) {
	if size < 1 {
		return nil, errors.Errorf("zero size")
	}

	last := indexHeight(size - 1)

	return func(yield func(uint64, iter.Seq[uint64]) bool) {
			if yield == nil {
				return
			}

			for i := range last + 1 {
				height := h(last, i)
				if !yield(height, indicesInHeight(size, height)) {
					return
				}
			}
		},
		nil
}

func traverseByHeight(size uint64) (iter.Seq2[uint64, iter.Seq[uint64]], error) {
	return baseTraverseByHeight(size, func(_ uint64, i uint64) uint64 { return i })
}

func reverseByHeight(size uint64) (iter.Seq2[uint64, iter.Seq[uint64]], error) {
	return baseTraverseByHeight(size, func(last, i uint64) uint64 { return last - i })
}

func AsyncIsValidNodesHash(ctx context.Context, semSize uint64, b []byte, nodes []Node) error {
	nnodes := uint64(len(nodes))

	switch {
	case nnodes < 1:
		return nil
	case nnodes < 2:
		return isValidNodeHash(b, 0, nodes)
	}

	var newJob dutil.JobWorkerNewJobFunc
	var jobDone func()
	var jobWait dutil.InErrorFunc[error]

	if semSize > nnodes {
		semSize = nnodes
	}

	switch n, d, w, err := dutil.NewFuncJobWorker(ctx, semSize); {
	case err != nil:
		return err
	default:
		newJob, jobDone, jobWait = n, d, w
	}

	for index := range nnodes {
		if err := newJob(func(context.Context) error {
			return isValidNodeHash(b, index, nodes)
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func isValidNodeHash(b []byte, index uint64, nodes []Node) error {
	n := nodes[index]
	if n == nil {
		return errors.Errorf("nil node at %d", index)
	}

	if err := n.IsValid(b); err != nil {
		return err
	}

	var children [2]Node

	switch c, err := childrenNodes(index, nodes); {
	case errors.Is(err, errNoChildren):
	case err != nil:
		return err
	default:
		children = c
	}

	switch h, _, err := n.SetHash(children[0], children[1]); {
	case err != nil:
		return err
	case !n.Hash().Equal(h):
		return errors.Errorf("hash does not match")
	default:
		return nil
	}
}
