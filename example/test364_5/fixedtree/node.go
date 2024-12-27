package fixedtree

import (
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

var emptyNodeBytes = []byte{0x0}

type Node interface {
	util.IsValider
	Key() string
	Hash() util.Hash
	Equal(Node) bool
	SetHash(left, right Node) (util.Hash, func() error, error)
}

type BaseNode struct {
	h   util.Hash
	key string
}

func NewBaseNode(key string) *BaseNode {
	return &BaseNode{key: key}
}

func (n *BaseNode) Key() string {
	return n.key
}

func (n *BaseNode) Hash() util.Hash {
	return n.h
}

func (n *BaseNode) SetHash(left, right Node) (util.Hash, func() error, error) {
	if n.key == "" {
		return nil, nil, errors.Errorf("empty key")
	}

	var lh, rh []byte

	switch {
	case left == nil:
		lh = emptyNodeBytes
	case left.Hash() == nil:
		return nil, nil, util.ErrInvalid.Errorf("nil left hash")
	default:
		lh = left.Hash().Bytes()
	}

	switch {
	case right == nil:
		lh = emptyNodeBytes
	case right.Hash() == nil:
		return nil, nil, util.ErrInvalid.Errorf("nil right hash")
	default:
		rh = right.Hash().Bytes()
	}

	h := valuehash.NewSHA256(slices.Concat([]byte(n.key), lh, rh))

	return h, func() error {
		if n.h != nil {
			return errors.Errorf("already hashed")
		}

		n.h = h

		return nil
	}, nil
}

func (n *BaseNode) isEmpty() bool {
	return n.key == "" && n.h == nil
}

func (n *BaseNode) Equal(b Node) bool {
	if b == nil {
		return n.isEmpty()
	}

	var nb *BaseNode

	switch i, ok := b.(*BaseNode); {
	case !ok:
		return false
	case i == nil:
		return n.isEmpty()
	default:
		nb = i
	}

	switch {
	case n.key != nb.key:
		return false
	case n.h == nil || nb.h == nil:
		return n.h == nil && nb.h == nil
	case !n.h.Equal(nb.h):
		return false
	}

	return true
}

func (n *BaseNode) IsValid(b []byte) error {
	return util.ErrInvalid.Wrap(IsValidNode(n, b))
}

type BaseNodeJSONMarshaler struct {
	Hash util.Hash `json:"hash,omitempty"`
	Key  string    `json:"key,omitempty"`
}

func (n *BaseNode) JSONMarshaler() BaseNodeJSONMarshaler {
	return BaseNodeJSONMarshaler{
		Key:  n.key,
		Hash: n.h,
	}
}

func (n *BaseNode) MarshalJSON() ([]byte, error) {
	if n == nil {
		return nil, nil
	}

	return util.MarshalJSON(n.JSONMarshaler())
}

type baseNodeJSONUnmarshaler struct {
	Hash valuehash.HashDecoder `json:"hash"`
	Key  string                `json:"key"`
}

func (n *BaseNode) UnmarshalJSON(b []byte) error {
	var u baseNodeJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "unmarshal BaseNode")
	}

	if u.Key == "" && u.Hash.Hash() == nil {
		n = nil

		return nil
	}

	n.key = u.Key
	n.h = u.Hash.Hash()

	return nil
}

func IsValidNode(n Node, b []byte) error {
	switch {
	case len(n.Key()) < 1:
		return errors.Errorf("empty key")
	case n.Hash() == nil:
		return errors.Errorf("nil node hash")
	default:
		if err := n.Hash().IsValid(nil); err != nil {
			return errors.WithMessage(err, "node")
		}
	}

	return nil
}
