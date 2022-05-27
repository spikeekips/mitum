package base

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

var (
	OperationFixedtreeHint              = hint.MustNewHint("operation-fixedtree-v0.0.1")
	BaseOperationProcessReasonErrorHint = hint.MustNewHint("operation-fixedtree-node-process-reason-v0.0.1")
)

type Operation interface {
	hint.Hinter
	util.IsValider
	util.Hasher
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
	reason OperationProcessReasonError
	fixedtree.BaseNode
	inState bool
}

func NewInStateOperationFixedtreeNode(facthash util.Hash, reason string) OperationFixedtreeNode {
	var operr OperationProcessReasonError
	if len(reason) > 0 {
		operr = NewBaseOperationProcessReasonError(reason)
	}

	return OperationFixedtreeNode{
		BaseNode: fixedtree.NewBaseNode(facthash.String()),
		inState:  true,
		reason:   operr,
	}
}

func NewNotInStateOperationFixedtreeNode(facthash util.Hash, reason string) OperationFixedtreeNode {
	var operr OperationProcessReasonError
	if len(reason) > 0 {
		operr = NewBaseOperationProcessReasonError(reason)
	}

	return OperationFixedtreeNode{
		BaseNode: fixedtree.NewBaseNode(facthash.String() + "-"),
		inState:  false,
		reason:   operr,
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

func (no OperationFixedtreeNode) SetHash(h util.Hash) fixedtree.Node {
	return OperationFixedtreeNode{
		BaseNode: no.BaseNode.SetHash(h).(fixedtree.BaseNode), //nolint:forcetypeassert // ...
		inState:  no.inState,
		reason:   no.reason,
	}
}

type operationFixedtreeNodeJSONMarshaler struct {
	Reason OperationProcessReasonError `json:"reason"`
	fixedtree.BaseNodeJSONMarshaler
}

func (no OperationFixedtreeNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(operationFixedtreeNodeJSONMarshaler{
		BaseNodeJSONMarshaler: no.BaseNode.JSONMarshaler(),
		Reason:                no.reason,
	})
}

type operationFixedtreeNodeJSONUnmarshaler struct {
	Reason json.RawMessage `json:"reason"`
}

func (no *OperationFixedtreeNode) DecodeJSON(b []byte, enc *jsonenc.Encoder) error {
	e := util.StringErrorFunc("failed to decode OperationFixedtreeNode")

	var ub fixedtree.BaseNode
	if err := enc.Unmarshal(b, &ub); err != nil {
		return e(err, "")
	}

	no.BaseNode = ub

	var u operationFixedtreeNodeJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e(err, "")
	}

	if err := encoder.Decode(enc, u.Reason, &no.reason); err != nil {
		return e(err, "")
	}

	return nil
}

var ErrNotChangedOperationProcessReason = NewBaseOperationProcessReasonError( //nolint:varcheck // .
	"states not changed")

type OperationProcessReasonError interface {
	error
	Msg() string
}

type BaseOperationProcessReasonError struct {
	id  string
	msg string
	hint.BaseHinter
}

func NewBaseOperationProcessReasonError(s string, a ...interface{}) BaseOperationProcessReasonError {
	f := util.FuncCaller(3)

	return BaseOperationProcessReasonError{
		BaseHinter: hint.NewBaseHinter(BaseOperationProcessReasonErrorHint),
		id:         fmt.Sprintf("%+v", f),
		msg:        fmt.Errorf(s, a...).Error(), //nolint:goerr113 // it just stores error message
	}
}

func (BaseOperationProcessReasonError) Hint() hint.Hint {
	return BaseOperationProcessReasonErrorHint
}

func (e BaseOperationProcessReasonError) Is(err error) bool {
	b, ok := err.(BaseOperationProcessReasonError) //nolint:errorlint // ...
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
	Message string `json:"message"`
	hint.BaseHinter
}

func (e BaseOperationProcessReasonError) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BaseOperationProcessReasonErrorJSONMarshaler{
		BaseHinter: e.BaseHinter,
		Message:    e.msg,
	})
}

type BaseOperationProcessReasonErrorJSONUnmarshaler struct {
	Message string `json:"message"`
}

func (e *BaseOperationProcessReasonError) UnmarshalJSON(b []byte) error {
	var u BaseOperationProcessReasonErrorJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BaseOperationProcessReasonError")
	}

	e.msg = u.Message

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
