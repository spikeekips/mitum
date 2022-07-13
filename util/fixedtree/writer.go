package fixedtree

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	noParentError   = util.NewError("no parent")
	noChildrenError = util.NewError("no children")
)

type NodeWrite func(uint64, Node) error

type Writer struct {
	nodes []Node
	ht    hint.Hint
	sync.RWMutex
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
	g.RLock()
	defer g.RUnlock()

	return len(g.nodes)
}

func (g *Writer) Root() util.Hash {
	g.RLock()
	defer g.RUnlock()

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
	g.shrinkNodes()

	g.RLock()
	defer g.RUnlock()

	if _, err := generateNodesHash(g.nodes, w); err != nil {
		return errors.WithMessage(err, "failed to write Tree")
	}

	return nil
}

func (g *Writer) Tree() (Tree, error) {
	g.shrinkNodes()

	g.RLock()
	defer g.RUnlock()

	return NewTree(g.ht, g.nodes)
}

func (g *Writer) Traverse(f func(index uint64, node Node) (bool, error)) error {
	g.RLock()
	defer g.RUnlock()

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

func (g *Writer) shrinkNodes() {
	g.Lock()
	defer g.Unlock()

	if len(g.nodes) < 1 {
		return
	}

	var n int64 = -1

	for i := range g.nodes {
		n = int64(len(g.nodes) - i)
		if g.nodes[n-1] != nil {
			break
		}
	}

	if int64(len(g.nodes)) == n {
		return
	}

	g.nodes = g.nodes[:n]
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
		return nil, err
	}

	switch h, err := nodeHash(n, children[0], children[1]); {
	case err != nil:
		return nil, err
	default:
		n = n.SetHash(h)
		nodes[index] = n
	}

	if err := w(index, nodes[index]); err != nil {
		return nil, err
	}

	return n, nil
}

func generateNodesHash(nodes []Node, w NodeWrite) ([]Node, error) {
	for i := uint64(0); i < uint64(len(nodes)); i++ {
		index := uint64(len(nodes)-1) - i

		n, err := generateNodeHash(index, nodes, w)
		if err != nil {
			return nil, err
		}

		nodes[index] = n
	}

	return nodes, nil
}
