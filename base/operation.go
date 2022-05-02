package base

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	OperationFixedTreeNodeHint          = hint.MustNewHint("operation-fixedtree-node-v0.0.1")
	BaseOperationProcessReasonErrorHint = hint.MustNewHint("operation-fixedtree-node-process-reason-v0.0.1")
)

type Operation interface {
	hint.Hinter // BLOCK remove Hinter from interface
	util.IsValider
	SignedFact
	PreProcess(context.Context, GetStateFunc) (OperationProcessReasonError, error)
	Process(context.Context, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error)
}

type OperationProcessorProcessFunc func(context.Context, Operation, GetStateFunc) (OperationProcessReasonError, error)

type OperationProcessor interface {
	PreProcess(context.Context, Operation, GetStateFunc) (OperationProcessReasonError, error)
	Process(context.Context, Operation, GetStateFunc) ([]StateMergeValue, OperationProcessReasonError, error)
}

type OperationFixedTreeNode struct {
	tree.BaseFixedTreeNode
	inState bool
	reason  OperationProcessReasonError
}

func NewOperationFixedTreeNode(
	index uint64,
	facthash util.Hash,
	inState bool,
	errorreason string,
) OperationFixedTreeNode {
	return NewOperationFixedTreeNodeWithHash(index, facthash, nil, inState, errorreason)
}

func NewOperationFixedTreeNodeWithHash(
	index uint64,
	facthash util.Hash,
	hash []byte,
	inState bool,
	reason string,
) OperationFixedTreeNode {
	var operr OperationProcessReasonError
	if len(reason) > 0 {
		operr = NewBaseOperationProcessReasonError(reason)
	}

	k := facthash.String()
	if !inState {
		k += "-"
	}

	return OperationFixedTreeNode{
		BaseFixedTreeNode: tree.NewBaseFixedTreeNodeWithHash(OperationFixedTreeNodeHint, index, k, hash),
		inState:           inState,
		reason:            operr,
	}
}

func (no OperationFixedTreeNode) InState() bool {
	_, instate := ParseTreeNodeOperationKey(no.Key())

	return instate
}

func (no OperationFixedTreeNode) Operation() util.Hash {
	h, _ := ParseTreeNodeOperationKey(no.Key())

	return h
}

func (no OperationFixedTreeNode) Reason() OperationProcessReasonError {
	return no.reason
}

func (no OperationFixedTreeNode) SetHash(h []byte) tree.FixedTreeNode {
	no.BaseFixedTreeNode = no.BaseFixedTreeNode.SetHash(h).(tree.BaseFixedTreeNode)

	return no
}

type operationFixedTreeNodeJSONMarshaler struct {
	tree.BaseFixedTreeNodeJSONMarshaler
	Reason OperationProcessReasonError `json:"reason"`
}

func (no OperationFixedTreeNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(operationFixedTreeNodeJSONMarshaler{
		BaseFixedTreeNodeJSONMarshaler: no.BaseFixedTreeNode.JSONMarshaler(),
		Reason:                         no.reason,
	})
}

type operationFixedTreeNodeJSONUnmarshaler struct {
	Reason json.RawMessage `json:"reason"`
}

func (no *OperationFixedTreeNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode OperationFixedTreeNode")

	var ub tree.BaseFixedTreeNode
	if err := enc.Unmarshal(b, &ub); err != nil {
		return e(err, "")
	}

	var u operationFixedTreeNodeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	switch hinter, err := enc.Decode(u.Reason); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
	default:
		i, ok := hinter.(OperationProcessReasonError)
		if !ok {
			return e(nil, "not OperationProcessReasonError, %T", hinter)
		}
		no.reason = i
	}

	no.BaseFixedTreeNode = ub

	return nil
}

var NotChangedOperationProcessReasonError = NewBaseOperationProcessReasonError("states not changed")

type OperationProcessReasonError interface {
	error
	hint.Hinter
	Msg() string
}

type BaseOperationProcessReasonError struct {
	hint.BaseHinter
	id  string
	msg string
}

func NewBaseOperationProcessReasonError(s string, a ...interface{}) BaseOperationProcessReasonError {
	f := util.FuncCaller(3)

	return BaseOperationProcessReasonError{
		BaseHinter: hint.NewBaseHinter(BaseOperationProcessReasonErrorHint),
		id:         fmt.Sprintf("%+v", f),
		msg:        fmt.Errorf(s, a...).Error(),
	}
}

func (BaseOperationProcessReasonError) Hint() hint.Hint {
	return BaseOperationProcessReasonErrorHint
}

func (e BaseOperationProcessReasonError) Is(err error) bool {
	b, ok := err.(BaseOperationProcessReasonError) // nolint:errorlint
	if !ok {
		return false
	}

	return e.id == b.id
}

func (e BaseOperationProcessReasonError) Msg() string {
	return e.msg
}

func (e BaseOperationProcessReasonError) Error() string {
	return e.msg
}

type BaseOperationProcessReasonErrorJSONMarshaler struct {
	hint.BaseHinter
	Msg string `json:"message"`
}

func (e BaseOperationProcessReasonError) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BaseOperationProcessReasonErrorJSONMarshaler{
		BaseHinter: e.BaseHinter,
		Msg:        e.msg,
	})
}

type BaseOperationProcessReasonErrorJSONUnmarshaler struct {
	Msg string `json:"message"`
}

func (e *BaseOperationProcessReasonError) UnmarshalJSON(b []byte) error {
	var u BaseOperationProcessReasonErrorJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BaseOperationProcessReasonError")
	}

	e.msg = u.Msg

	return nil
}

func ParseTreeNodeOperationKey(s string) (util.Hash, bool) {
	k := s

	var notInState bool
	if strings.HasSuffix(s, "-") {
		k = k[:len(k)-1]
		notInState = true
	}

	return valuehash.NewBytesFromString(k), !notInState
}
