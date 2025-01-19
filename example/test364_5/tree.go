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

type stateTreeNode struct {
	*fixedtree.BaseNode           // NOTE state key is key
	h                   util.Hash // NOTE state hash
	b                   []byte
}

func newStateTreeNode(stateKey string, h util.Hash) (*stateTreeNode, error) {
	if h == nil {
		return nil, errors.Errorf("nil state hash")
	}

	return &stateTreeNode{
			BaseNode: fixedtree.NewBaseNode(stateKey),
			h:        h,
			b:        slices.Concat([]byte(stateKey), h.Bytes()),
		},
		nil
}

func (n *stateTreeNode) StateHash() util.Hash {
	return n.h
}

func (n *stateTreeNode) IsValid(b []byte) error {
	if n.h == nil {
		return util.ErrInvalid.Errorf("nil state hash")
	}

	if err := fixedtree.IsValidNode(n, b); err != nil {
		return err
	}

	return n.h.IsValid(nil)
}

func (n *stateTreeNode) SetHash(left, right fixedtree.Node) (util.Hash, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return n.b, nil
	}); {
	case err != nil:
		return nil, nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}

type operationTreeNode struct {
	*fixedtree.BaseNode // NOTE Operation Hash is key
	fact                util.Hash
	reason              string
	b                   []byte
}

func newOperationTreeNode(oph, fact util.Hash, reason string) (*operationTreeNode, error) {
	switch {
	case oph == nil:
		return nil, errors.Errorf("nil oph")
	case fact == nil:
		return nil, errors.Errorf("nil fact")
	}

	key := oph.String()

	return &operationTreeNode{
			BaseNode: fixedtree.NewBaseNode(key),
			fact:     fact,
			reason:   reason,
			b: slices.Concat(
				[]byte(key),
				fact.Bytes(),
				util.BoolToBytes(reason == ""), // NOTE in state
			),
		},
		nil
}

func (n *operationTreeNode) Fact() util.Hash {
	return n.fact
}

func (n *operationTreeNode) Reason() string {
	return n.reason
}

func (n *operationTreeNode) SetReason(s string) {
	if n.reason == s {
		return
	}

	n.reason = s

	n.b = slices.Concat(
		[]byte(n.Key()),
		n.fact.Bytes(),
		util.BoolToBytes(n.reason == ""),
	)
}

func (n *operationTreeNode) IsValid(b []byte) error {
	if n.fact == nil {
		return util.ErrInvalid.Errorf("nil fact")
	}

	if err := fixedtree.IsValidNode(n, b); err != nil {
		return err
	}

	return n.fact.IsValid(nil)
}

func (n *operationTreeNode) SetHash(left, right fixedtree.Node) (util.Hash, dutil.ErrorFunc, error) {
	switch h, err := fixedtree.MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return n.b, nil
	}); {
	case err != nil:
		return nil, nil, err
	default:
		return h, n.BaseNode.UpdateHashFunc(h), nil
	}
}
