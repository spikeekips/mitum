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
	OperationFixedtreeNodeHint          = hint.MustNewHint("operation-fixedtree-node-v0.0.1")
	BaseOperationProcessReasonErrorHint = hint.MustNewHint("operation-fixedtree-node-process-reason-v0.0.1")
)

type Operation interface {
	hint.Hinter
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

type OperationFixedtreeNode struct {
	tree.BaseFixedtreeNode
	inState bool
	reason  OperationProcessReasonError
}

func NewOperationFixedtreeNode(
	index uint64,
	facthash util.Hash,
	inState bool,
	errorreason string,
) OperationFixedtreeNode {
	return NewOperationFixedtreeNodeWithHash(index, facthash, nil, inState, errorreason)
}

func NewOperationFixedtreeNodeWithHash(
	index uint64,
	facthash util.Hash,
	hash []byte,
	inState bool,
	reason string,
) OperationFixedtreeNode {
	var operr OperationProcessReasonError
	if len(reason) > 0 {
		operr = NewBaseOperationProcessReasonError(reason)
	}

	k := facthash.String()
	if !inState {
		k += "-"
	}

	return OperationFixedtreeNode{
		BaseFixedtreeNode: tree.NewBaseFixedtreeNodeWithHash(OperationFixedtreeNodeHint, index, k, hash),
		inState:           inState,
		reason:            operr,
	}
}

func (no OperationFixedtreeNode) InState() bool {
	_, instate := ParseTreeNodeOperationKey(no.Key())

	return instate
}

func (no OperationFixedtreeNode) Operation() util.Hash {
	h, _ := ParseTreeNodeOperationKey(no.Key())

	return h
}

func (no OperationFixedtreeNode) Reason() OperationProcessReasonError {
	return no.reason
}

func (no OperationFixedtreeNode) SetHash(h []byte) tree.FixedtreeNode {
	no.BaseFixedtreeNode = no.BaseFixedtreeNode.SetHash(h).(tree.BaseFixedtreeNode)

	return no
}

type operationFixedtreeNodeJSONMarshaler struct {
	tree.BaseFixedtreeNodeJSONMarshaler
	Reason OperationProcessReasonError `json:"reason"`
}

func (no OperationFixedtreeNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(operationFixedtreeNodeJSONMarshaler{
		BaseFixedtreeNodeJSONMarshaler: no.BaseFixedtreeNode.JSONMarshaler(),
		Reason:                         no.reason,
	})
}

type operationFixedtreeNodeJSONUnmarshaler struct {
	Reason json.RawMessage `json:"reason"`
}

func (no *OperationFixedtreeNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode OperationFixedtreeNode")

	var ub tree.BaseFixedtreeNode
	if err := enc.Unmarshal(b, &ub); err != nil {
		return e(err, "")
	}

	var u operationFixedtreeNodeJSONUnmarshaler
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

	no.BaseFixedtreeNode = ub

	return nil
}

var NotChangedOperationProcessReasonError = NewBaseOperationProcessReasonError("states not changed")

type OperationProcessReasonError interface {
	error
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
