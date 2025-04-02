package dop5

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	OperationsTreeHint = hint.MustNewHint("operations-tree-v0.0.1")
	StatesTreeHint     = hint.MustNewHint("states-tree-v0.0.1")
)

func NewOperationsTree(nodes []fixedtree.Node) *fixedtree.Tree {
	return fixedtree.NewTree(OperationsTreeHint, nodes)
}

func NewStatesTree(nodes []fixedtree.Node) *fixedtree.Tree {
	return fixedtree.NewTree(StatesTreeHint, nodes)
}

type StateTreeNode struct {
	*fixedtree.BaseNode           // NOTE state key is key
	h                   util.Hash // NOTE state hash
	b                   []byte
}

func NewStateTreeNode(stateKey string, h util.Hash) (*StateTreeNode, error) {
	if h == nil {
		return nil, errors.Errorf("nil state hash")
	}

	return &StateTreeNode{
			BaseNode: fixedtree.NewBaseNode(stateKey),
			h:        h,
			b:        slices.Concat([]byte(stateKey), h.Bytes()),
		},
		nil
}

func (n *StateTreeNode) StateHash() util.Hash {
	return n.h
}

func (n *StateTreeNode) IsValid(b []byte) error {
	if n.h == nil {
		return util.ErrInvalid.Errorf("nil state hash")
	}

	if err := fixedtree.IsValidNode(n, b); err != nil {
		return err
	}

	return n.h.IsValid(nil)
}

func (n *StateTreeNode) SetHash(left, right fixedtree.Node) (string, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return n.b, nil
	}); {
	case err != nil:
		return "", nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}

type OperationTreeNode struct {
	*fixedtree.BaseNode // NOTE Operation Hash is key
	fact                util.Hash
	reason              string
	b                   []byte
}

func NewOperationTreeNode(oph, facth util.Hash, reason string) (*OperationTreeNode, error) {
	switch {
	case oph == nil:
		return nil, errors.Errorf("nil oph")
	case facth == nil:
		return nil, errors.Errorf("nil fact")
	}

	key := oph.String()

	return &OperationTreeNode{
			BaseNode: fixedtree.NewBaseNode(key),
			fact:     facth,
			reason:   reason,
			b: slices.Concat(
				[]byte(key),
				facth.Bytes(),
				dutil.EncodeBool(reason == ""), // NOTE in state
			),
		},
		nil
}

func (n *OperationTreeNode) Fact() util.Hash {
	return n.fact
}

func (n *OperationTreeNode) Reason() string {
	return n.reason
}

func (n *OperationTreeNode) SetReason(s string) {
	if n.reason == s {
		return
	}

	n.reason = s

	n.b = slices.Concat(
		[]byte(n.Key()),
		n.fact.Bytes(),
		dutil.EncodeBool(n.reason == ""),
	)
}

func (n *OperationTreeNode) IsValid(b []byte) error {
	if n.fact == nil {
		return util.ErrInvalid.Errorf("nil fact")
	}

	if err := fixedtree.IsValidNode(n, b); err != nil {
		return err
	}

	return n.fact.IsValid(nil)
}

func (n *OperationTreeNode) SetHash(left, right fixedtree.Node) (string, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return n.b, nil
	}); {
	case err != nil:
		return "", nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}
