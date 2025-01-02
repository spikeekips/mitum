package main

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

func newOperationsTree(nodes []fixedtree.Node) *fixedtree.Tree {
	return fixedtree.NewTree(OperationsTreeHint, nodes)
}

func newStatesTree(nodes []fixedtree.Node) *fixedtree.Tree {
	return fixedtree.NewTree(StatesTreeHint, nodes)
}

type StateTreeNode struct {
	*fixedtree.BaseNode // NOTE state key is key
	h                   util.Hash
}

func NewStateTreeNode(stateKey string, h util.Hash) *StateTreeNode {
	return &StateTreeNode{
		BaseNode: fixedtree.NewBaseNode(stateKey),
		h:        h,
	}
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

func (n *StateTreeNode) SetHash(left, right fixedtree.Node) (util.Hash, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		switch {
		case n.h == nil:
			return nil, errors.Errorf("nil state hash")
		default:
			return slices.Concat([]byte(n.Key()), n.h.Bytes()), nil
		}
	}); {
	case err != nil:
		return nil, nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}

type OperationTreeNode struct {
	*fixedtree.BaseNode // NOTE Operation Hash is key
	fact                util.Hash
	reason              string
}

func NewOperationTreeNode(oph, fact util.Hash, reason string) *OperationTreeNode {
	var key string

	if oph != nil {
		key = oph.String()
	}

	return &OperationTreeNode{
		BaseNode: fixedtree.NewBaseNode(key),
		fact:     fact,
		reason:   reason,
	}
}

func (n *OperationTreeNode) Fact() util.Hash {
	return n.fact
}

func (n *OperationTreeNode) Reason() string {
	return n.reason
}

func (n *OperationTreeNode) SetReason(s string) {
	if n.reason != s {
		n.reason = s
	}
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

func (n *OperationTreeNode) SetHash(left, right fixedtree.Node) (util.Hash, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		switch {
		case n.fact == nil:
			return nil, errors.Errorf("nil fact")
		default:
			return slices.Concat(
				[]byte(n.Key()),
				n.fact.Bytes(),
			), nil
		}
	}); {
	case err != nil:
		return nil, nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}
