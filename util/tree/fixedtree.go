package tree

import (
	"bytes"
	"crypto/sha256"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var FixedtreeHint = hint.MustNewHint("fixedtree-v0.0.1")

var (
	noParentError     = util.NewError("node has no parent")
	noChildrenError   = util.NewError("node has no children")
	InvalidProofError = util.NewError("invalid proof")
)

type FixedtreeNode interface {
	util.IsValider
	Index() uint64
	Key() string
	Hash() []byte
	SetHash([]byte) FixedtreeNode
	Equal(FixedtreeNode) bool
}

type BaseFixedtreeNode struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	index uint64
	key   string
	hash  []byte
}

func NewBaseFixedtreeNode(ht hint.Hint, index uint64, key string) BaseFixedtreeNode {
	return BaseFixedtreeNode{
		BaseHinter: hint.NewBaseHinter(ht),
		index:      index,
		key:        key,
	}
}

func NewBaseFixedtreeNodeWithHash(ht hint.Hint, index uint64, key string, hash []byte) BaseFixedtreeNode {
	tr := NewBaseFixedtreeNode(ht, index, key)
	tr.hash = hash

	return tr
}

func (no BaseFixedtreeNode) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BaseFixedtreeNode")
	if err := no.BaseHinter.IsValid(nil); err != nil {
		return e(err, "")
	}

	switch {
	case len(no.key) < 1:
		return e(util.InvalidError.Errorf("empty key"), "")
	case len(no.hash) < 1:
		return e(util.InvalidError.Errorf("empty hash"), "")
	}

	return nil
}

func (no BaseFixedtreeNode) Equal(n FixedtreeNode) bool {
	switch {
	case no.index != n.Index():
		return false
	case no.key != n.Key():
		return false
	case !bytes.Equal(no.hash, n.Hash()):
		return false
	}

	return true
}

func (no BaseFixedtreeNode) Index() uint64 {
	return no.index
}

func (no BaseFixedtreeNode) Key() string {
	return no.key
}

func (no BaseFixedtreeNode) Hash() []byte {
	return no.hash
}

func (no BaseFixedtreeNode) SetHash(h []byte) FixedtreeNode {
	no.hash = h

	return no
}

type Fixedtree struct {
	util.DefaultJSONMarshaled
	hint.BaseHinter
	nodes []FixedtreeNode
}

func EmptyFixedtree() Fixedtree {
	return Fixedtree{BaseHinter: hint.NewBaseHinter(FixedtreeHint)}
}

func NewFixedtree(nodes []FixedtreeNode) Fixedtree {
	return NewFixedtreeWithHint(FixedtreeHint, nodes)
}

func NewFixedtreeWithHint(ht hint.Hint, nodes []FixedtreeNode) Fixedtree {
	return Fixedtree{BaseHinter: hint.NewBaseHinter(ht), nodes: nodes}
}

func (tr Fixedtree) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid Fixedtree")
	if err := tr.BaseHinter.IsValid(FixedtreeHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if tr.Len() < 1 {
		return nil
	}

	ns := make([]util.IsValider, len(tr.nodes))

	for i := range tr.nodes {
		i := i
		ns[i] = util.DummyIsValider(func([]byte) error {
			n := tr.nodes[i]
			if n == nil {
				return util.InvalidError.Errorf("empty FixedtreeNode")
			}

			if err := tr.nodes[i].IsValid(nil); err != nil {
				return err
			}

			if int(n.Index()) != i {
				return util.InvalidError.Errorf("wrong index; %d != %d", n.Index(), i)
			}

			return nil
		})
	}

	if err := util.CheckIsValid(nil, false, ns...); err != nil {
		return e(err, "")
	}

	for i := range tr.nodes {
		n := tr.nodes[i]
		h, err := tr.generateNodeHash(n)
		if err != nil {
			return e(util.InvalidError.Wrap(err), "")
		}

		if !bytes.Equal(n.Hash(), h) {
			return e(util.InvalidError.Errorf("wrong node hash"), "")
		}
	}

	return nil
}

func (tr Fixedtree) Len() int {
	return len(tr.nodes)
}

// Root returns hash of top node
func (tr Fixedtree) Root() []byte {
	if tr.Len() < 1 {
		return nil
	}

	return tr.nodes[0].Hash()
}

func (tr Fixedtree) Node(index uint64) (FixedtreeNode, error) {
	if int(index) >= tr.Len() {
		return nil, util.NotFoundError.Errorf("node, %d not found", index)
	}

	return tr.nodes[index], nil
}

func (tr Fixedtree) Traverse(f func(FixedtreeNode) (bool, error)) error {
	for i := range tr.nodes {
		if keep, err := f(tr.nodes[i]); err != nil {
			return err
		} else if !keep {
			return nil
		}
	}

	return nil
}

// Proof returns the nodes to prove whether node is in tree. It always returns
// root node + N(2 children).
func (tr Fixedtree) Proof(index uint64) ([]FixedtreeNode, error) {
	e := util.StringErrorFunc("failed to make proof")

	self, err := tr.Node(index)
	if err != nil {
		return nil, e(err, "")
	}

	if tr.Len() < 1 {
		return nil, nil
	}

	height := heightFixedtree(index)

	parents := make([]FixedtreeNode, height+1)
	parents[0] = self

	l := index
	var i int
	for {
		j, err := tr.parent(l)
		if err != nil {
			if errors.Is(err, noParentError) {
				break
			}

			return nil, e(err, "")
		}
		parents[i+1] = j
		l = j.Index()
		i++
	}

	pr := make([]FixedtreeNode, (height+1)*2+1)
	for i := range parents {
		n := parents[i]
		if cs, err := tr.children(n.Index()); err != nil {
			if !errors.Is(err, noChildrenError) {
				return nil, e(err, "")
			}
		} else {
			pr[(i * 2)] = cs[0]
			pr[(i*2)+1] = cs[1]
		}
	}
	pr[len(pr)-1] = tr.nodes[0]

	return pr, nil
}

func (tr Fixedtree) children(index uint64) ([]FixedtreeNode, error) {
	i, err := childrenFixedtree(tr.Len(), index)
	if err != nil {
		return nil, err
	}
	if i[1] == 0 {
		return []FixedtreeNode{tr.nodes[i[0]], nil}, nil
	}
	return []FixedtreeNode{tr.nodes[i[0]], tr.nodes[i[1]]}, nil
}

func (tr Fixedtree) parent(index uint64) (FixedtreeNode, error) {
	var n FixedtreeNode
	i, err := parentFixedtree(index)
	if err != nil {
		return n, err
	}
	return tr.Node(i)
}

// generateNodeHash generates node hash. Hash was derived from index and key.
func (tr Fixedtree) generateNodeHash(n FixedtreeNode) ([]byte, error) {
	e := util.StringErrorFunc("failed to generate node hash")

	if n == nil || len(n.Key()) < 1 {
		return nil, e(nil, "node has empty key")
	}

	var left, right FixedtreeNode
	if i, err := tr.children(n.Index()); err != nil {
		if !errors.Is(err, noChildrenError) {
			return nil, e(err, "")
		}
	} else {
		left = i[0]
		right = i[1]
	}

	b, err := fixedtreeNodeHash(n, left, right)
	if err != nil {
		return nil, e(err, "")
	}

	return b, nil
}

type FixedtreeGenerator struct {
	sync.RWMutex
	Fixedtree
	size uint64
}

func NewFixedtreeGenerator(size uint64) *FixedtreeGenerator {
	return &FixedtreeGenerator{
		Fixedtree: NewFixedtree(make([]FixedtreeNode, size)),
		size:      size,
	}
}

func (tr *FixedtreeGenerator) Add(n FixedtreeNode) error {
	tr.Lock()
	defer tr.Unlock()

	e := util.StringErrorFunc("failed add to Fixedtree")

	if len(n.Key()) < 1 {
		return e(nil, "node has empty key")
	}

	if n.Index() >= tr.size {
		return e(nil, "out of range; index=%d, size=%d", n.Index(), tr.size)
	}

	tr.nodes[n.Index()] = n.SetHash(nil)

	return nil
}

func (tr *FixedtreeGenerator) Tree() (Fixedtree, error) {
	tr.RLock()
	defer tr.RUnlock()

	e := util.StringErrorFunc("failed to generate Fixedtree")

	if tr.size < 1 {
		return NewFixedtree(tr.nodes), nil
	}
	for i := range tr.nodes {
		if tr.nodes[i] == nil {
			return Fixedtree{}, e(nil, "empty node found, %d", i)
		}
	}

	if tr.size > 0 && len(tr.nodes[0].Hash()) < 1 {
		for i := range tr.nodes {
			n := tr.nodes[len(tr.nodes)-i-1]
			h, err := tr.generateNodeHash(n)
			if err != nil {
				return Fixedtree{}, e(err, "")
			}
			tr.nodes[n.Index()] = n.SetHash(h)
		}
	}

	return NewFixedtree(tr.nodes), nil
}

func fixedtreeNodeHash(
	self, // self node
	left, // left child
	right FixedtreeNode, // right child
) ([]byte, error) {
	if len(self.Key()) < 1 {
		return nil, errors.Errorf("node has empty key")
	}

	bi := util.Uint64ToBytes(self.Index())
	key := []byte(self.Key())
	a := make([]byte, len(key)+len(bi))
	copy(a, bi)
	copy(a[len(bi):], key)

	var lh, rh []byte
	if left != nil {
		lh = left.Hash()
	}
	if right != nil {
		rh = right.Hash()
	}

	return hashNode(util.ConcatBytesSlice(a, lh, rh)), nil
}

func ProveFixedtreeProof(pr []FixedtreeNode) error {
	if err := proveFixedtreeProof(pr); err != nil {
		return InvalidProofError.Wrap(err)
	}

	return nil
}

func proveFixedtreeProof(pr []FixedtreeNode) error {
	e := util.StringErrorFunc("failed to prove fixed tree proof")

	switch n := len(pr); {
	case n < 1:
		return e(nil, "nothing to prove")
	case n%2 != 1:
		return e(nil, "invalid proof; len=%d", n)
	case pr[len(pr)-1].Index() != 0:
		return e(nil, "root node not found")
	}

	for i := range pr {
		if err := pr[i].IsValid(nil); err != nil {
			return e(err, "node, %d", i)
		}
	}

	for i := 0; i < len(pr[:len(pr)-1])/2; i++ {
		a, b := pr[(i*2)], pr[(i*2)+1]
		if p, err := parentNodeInProof(i, pr, a.Index()); err != nil {
			return e(err, "nodes, %d and %d", a.Index(), b.Index())
		} else if h, err := fixedtreeNodeHash(p, a, b); err != nil {
			return e(err, "")
		} else if !bytes.Equal(p.Hash(), h) {
			return e(nil, "node, %d has wrong hash", p.Index())
		}
	}

	return nil
}

func parentNodeInProof(i int, pr []FixedtreeNode, index uint64) (FixedtreeNode, error) {
	var p FixedtreeNode
	switch j, err := parentFixedtree(index); {
	case err != nil:
		return p, err
	case i < (len(pr[:len(pr)-1])/2)-1:
		pa, pb := pr[(i*2)+2], pr[(i*2)+2+1]
		if j == pa.Index() {
			p = pa
		} else {
			p = pb
		}
	default:
		p = pr[len(pr)-1]
	}

	if len(p.Key()) < 1 {
		return p, errors.Errorf("parent node not found")
	}

	return p, nil
}

func heightFixedtree(index uint64) uint64 {
	if index == 0 {
		return 0
	}

	return uint64(math.Log(float64(index+1)) / math.Log(2))
}

func parentFixedtree(index uint64) (uint64, error) {
	height := heightFixedtree(index)
	if height == 0 {
		return 0, noParentError.Call()
	}

	currentFirst := uint64(math.Pow(2, float64(height)) - 1)
	pos := index - currentFirst

	if pos%2 == 1 {
		pos--
	}

	upFirst := uint64(math.Pow(2, float64(height-1)) - 1)
	return upFirst + pos/2, nil
}

func childrenFixedtree(size int, index uint64) ([]uint64, error) {
	height := heightFixedtree(index)

	currentFirst := uint64(math.Pow(2, float64(height)) - 1)
	pos := index - currentFirst
	nextFirst := uint64(math.Pow(2, float64(height+1)) - 1)

	children := make([]uint64, 2)
	i := nextFirst + pos*2
	if i >= uint64(size) {
		return nil, noChildrenError.Call()
	}
	children[0] = i

	if i := nextFirst + pos*2 + 1; i < uint64(size) {
		children[1] = i
	}

	return children, nil
}

func hashNode(b []byte) []byte {
	h := sha256.Sum256(b)

	return h[:]
}
