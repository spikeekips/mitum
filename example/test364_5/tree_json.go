package main

import (
	"github.com/spikeekips/mitum/example/test364_5/fixedtree"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type stateTreeNodeJSONMarshaler struct {
	H valuehash.HashDecoder `json:"state_hash,omitempty"`
}

func (n *StateTreeNode) MarshalJSON() ([]byte, error) {
	if n == nil {
		return nil, nil
	}

	return util.MarshalJSON(struct {
		fixedtree.BaseNodeJSONMarshaler
		stateTreeNodeJSONMarshaler
	}{
		BaseNodeJSONMarshaler: n.BaseNode.JSONMarshaler(),
		stateTreeNodeJSONMarshaler: stateTreeNodeJSONMarshaler{
			H: valuehash.NewHashDecoder(n.h),
		},
	})
}

func (n *StateTreeNode) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &n.BaseNode); err != nil {
		return err
	}

	var u stateTreeNodeJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	n.h = u.H.Hash()

	return nil
}

type operationTreeNodeJSONMarshaler struct {
	Fact   valuehash.HashDecoder `json:"fact,omitempty"`
	Reason string                `json:"reason,omitempty"`
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
			Fact:   valuehash.NewHashDecoder(n.fact),
			Reason: n.reason,
		},
	})
}

func (n *OperationTreeNode) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &n.BaseNode); err != nil {
		return err
	}

	var u operationTreeNodeJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	n.fact = u.Fact.Hash()
	n.reason = u.Reason

	return nil
}
