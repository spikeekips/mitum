package fixedtree

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	noParentError     = util.NewError("no parent")
	noChildrenError   = util.NewError("no children")
	InvalidProofError = util.NewError("invalid proof")
)

type NodeWrite func(uint64, Node) error

type Writer struct {
	sync.RWMutex
	ht    hint.Hint
	nodes []Node
}

func NewWriter(ht hint.Hint, size uint64) (*Writer, error) {
	if size < 1 {
		return nil, errors.Errorf("zero size")
	}

	return &Writer{
		ht:    ht,
		nodes: make([]Node, size),
	}, nil
}

func (g *Writer) Hint() hint.Hint {
	return g.ht
}

func (g *Writer) Len() int {
	return len(g.nodes)
}

func (g *Writer) Root() util.Hash {
	if len(g.nodes) < 1 {
		return nil
	}

	return g.nodes[0].Hash()
}

func (g *Writer) Add(index uint64, n Node) error {
	if index >= uint64(len(g.nodes)) {
		return errors.Errorf("failed add to Tree; out of range")
	}

	g.nodes[index] = n.SetHash(nil)

	return nil
}

func (g *Writer) Write(w NodeWrite) (err error) {
	if _, err := generateNodesHash(g.nodes, w); err != nil {
		return errors.Wrap(err, "failed to write Tree")
	}

	return nil
}

func (g *Writer) Tree() (Tree, error) {
	return NewTree(g.ht, g.nodes)
}

func (g *Writer) Traverse(f func(index uint64, node Node) (bool, error)) error {
	for i := range g.nodes {
		n := g.nodes[i]
		if n == nil {
			return errors.Errorf("empty node found at %d", i)
		}

		switch keep, err := f(uint64(i), n); {
		case err != nil:
			return err
		case !keep:
			return nil
		}
	}

	return nil
}

func generateNodeHash(index uint64, nodes []Node, w NodeWrite) (Node, error) {
	var n Node
	switch j := nodes[index]; {
	case j == nil:
		return nil, errors.Errorf("empty node")
	default:
		n = j
	}

	children, err := childrenNodes(nodes, index)
	switch {
	case err == nil:
	case errors.Is(err, noChildrenError):
	default:
		return nil, errors.Wrap(err, "")
	}

	switch h, err := nodeHash(n, children[0], children[1]); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		n = n.SetHash(h)
		nodes[index] = n
	}

	if err := w(index, nodes[index]); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return n, nil
}

func generateNodesHash(nodes []Node, w NodeWrite) ([]Node, error) {
	for i := uint64(0); i < uint64(len(nodes)); i++ {
		index := uint64(len(nodes)-1) - i

		n, err := generateNodeHash(index, nodes, w)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		nodes[index] = n
	}

	return nodes, nil
}
