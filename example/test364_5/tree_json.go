package dop5

import (
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type stateTreeNodeJSONMarshaler struct {
	H util.Hash `json:"state_hash,omitempty"`
}

func (n *StateTreeNode) MarshalJSON() ([]byte, error) {
	if n == nil {
		return nil, nil
	}

	return util.MarshalJSON(struct {
		stateTreeNodeJSONMarshaler
		fixedtree.BaseNodeJSONMarshaler
	}{
		BaseNodeJSONMarshaler: n.BaseNode.JSONMarshaler(),
		stateTreeNodeJSONMarshaler: stateTreeNodeJSONMarshaler{
			H: n.h,
		},
	})
}

func (n *StateTreeNode) UnmarshalJSON(b []byte) error {
	var m map[string]any
	if err := util.UnmarshalJSON(b, &m); err != nil {
		return err
	}

	if err := fixedtree.BaseNodeDecodeJSON(m, &n.BaseNode); err != nil {
		return err
	}

	return dutil.AssignJSONHash(m["state_hash"], &n.h)()
}

type operationTreeNodeJSONMarshaler struct {
	Fact   util.Hash `json:"fact,omitempty"`
	Reason string    `json:"reason,omitempty"`
}

func (n *OperationTreeNode) MarshalJSON() ([]byte, error) {
	if n == nil {
		return nil, nil
	}

	return util.MarshalJSON(struct {
		fixedtree.BaseNodeJSONMarshaler
		operationTreeNodeJSONMarshaler
	}{
		BaseNodeJSONMarshaler: n.BaseNode.JSONMarshaler(),
		operationTreeNodeJSONMarshaler: operationTreeNodeJSONMarshaler{
			Fact:   n.fact,
			Reason: n.reason,
		},
	})
}

func (n *OperationTreeNode) UnmarshalJSON(b []byte) error {
	var m map[string]any
	if err := util.UnmarshalJSON(b, &m); err != nil {
		return err
	}

	if err := fixedtree.BaseNodeDecodeJSON(m, &n.BaseNode); err != nil {
		return err
	}

	return dutil.FuncChain(
		dutil.AssignJSONHash(m["fact"], &n.fact),
		dutil.AssignJSON(m["reason"], &n.reason),
	)
}
