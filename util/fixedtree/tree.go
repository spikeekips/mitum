package fixedtree

import (
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type Tree struct {
	nodes []Node
	hint.BaseHinter
}

func EmptyTree() Tree {
	return Tree{}
}

func NewTree(ht hint.Hint, nodes []Node) (Tree, error) {
	if len(nodes) < 1 {
		return Tree{}, errors.Errorf("empty ndoes")
	}

	return Tree{
		BaseHinter: hint.NewBaseHinter(ht),
		nodes:      nodes,
	}, nil
}

func (t Tree) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid Tree")

	if t.Len() < 1 {
		return nil
	}

	if err := t.BaseHinter.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := t.Traverse(func(index uint64, n Node) (bool, error) {
		if err := n.IsValid(b); err != nil {
			return false, err
		}

		children, err := childrenNodes(t.nodes, index)

		switch {
		case err == nil:
		case errors.Is(err, errNoChildren):
		default:
			return false, util.ErrInvalid.Wrap(err)
		}

		switch h, err := nodeHash(n, children[0], children[1]); {
		case err != nil:
			return false, err
		case !n.Hash().Equal(h):
			return false, errors.Errorf("hash does not match")
		}

		return true, nil
	}); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (t Tree) Len() int {
	return len(t.nodes)
}

func (t Tree) Nodes() []Node {
	return t.nodes
}

func (t Tree) Node(index uint64) Node {
	if index >= uint64(t.Len()) {
		return nil
	}

	return t.nodes[index]
}

func (t Tree) Root() util.Hash {
	return t.nodes[0].Hash()
}

func (t Tree) Traverse(f func(index uint64, node Node) (bool, error)) error {
	for index := uint64(0); index < uint64(len(t.nodes)); index++ {
		n := t.nodes[index]
		if n == nil {
			return errors.Errorf("empty node found at %d", index)
		}

		switch keep, err := f(index, n); {
		case err != nil:
			return err
		case !keep:
			return nil
		}
	}

	return nil
}

func (t *Tree) Set(index uint64, n Node) error {
	if index >= uint64(len(t.nodes)) {
		return errors.Errorf("over size")
	}

	t.nodes[index] = n

	return nil
}

func (t Tree) Proof(key string) (Proof, error) {
	return NewProofFromNodes(t.nodes, key)
}

func childrenNodes(nodes []Node, index uint64) (c [2]Node, err error) {
	i, err := children(len(nodes), index)
	if err != nil {
		return c, errors.WithMessage(err, "children node hashes")
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

func children(size int, index uint64) (c [2]uint64, err error) {
	height := indexHeight(index)

	currentFirst := uint64(math.Pow(2, float64(height)) - 1)
	pos := index - currentFirst
	nextFirst := uint64(math.Pow(2, float64(height+1)) - 1)

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

func nodeHash(self, left, right Node) (util.Hash, error) {
	if len(self.Key()) < 1 {
		return nil, errors.Errorf("empty key")
	}

	key := []byte(self.Key())

	var lh, rh []byte
	if left != nil {
		lh = left.Hash().Bytes()
	}

	if right != nil {
		rh = right.Hash().Bytes()
	}

	return valuehash.NewSHA256(util.ConcatBytesSlice(key, lh, rh)), nil
}
