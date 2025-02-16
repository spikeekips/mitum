package fixedtree

import (
	"github.com/pkg/errors"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

var emptyNodeBytes = []byte{0x0}

type Node interface {
	util.IsValider
	Key() string
	Hash() util.Hash
	Equal(Node) bool
	SetHash(left, right Node) (util.Hash, dutil.ErrorFunc, error)
}

type BaseNode struct {
	h   util.Hash
	key string
}

func NewBaseNode(key string) *BaseNode {
	return &BaseNode{key: key}
}

func (n *BaseNode) Key() string {
	if n == nil {
		return ""
	}

	return n.key
}

func (n *BaseNode) Hash() util.Hash {
	if n == nil {
		return nil
	}

	return n.h
}

func (n *BaseNode) UpdateHashFunc(h util.Hash) dutil.ErrorFunc {
	return func() error {
		if n.h != nil {
			return errors.Errorf("already hashed")
		}

		n.h = h

		return nil
	}
}

func (n *BaseNode) SetHash(left, right Node) (util.Hash, dutil.ErrorFunc, error) {
	switch h, err := MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return []byte(n.key), nil
	}); {
	case err != nil:
		return nil, nil, err
	default:
		return h, n.UpdateHashFunc(h), nil
	}
}

func (n *BaseNode) Equal(b Node) bool {
	return IsEqualNode(n, b)
}

func (n *BaseNode) IsValid(b []byte) error {
	return IsValidNode(n, b)
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
		return util.ErrInvalid.Errorf("empty key")
	case n.Hash() == nil:
		return util.ErrInvalid.Errorf("nil node hash")
	default:
		if err := n.Hash().IsValid(nil); err != nil {
			return util.ErrInvalid.WithMessage(err, "node")
		}
	}

	return nil
}

func MakeBaseNodeHash(self, left, right Node, nodeHashBytes func() ([]byte, error)) (util.Hash, error) {
	if self.Key() == "" {
		return nil, errors.Errorf("empty key")
	}

	w, sum := dutil.NewSHA256HashWriter()
	defer sum()

	switch b, err := nodeHashBytes(); {
	case err != nil:
		return nil, err
	default:
		w(b)
	}

	switch {
	case left == nil:
		w(emptyNodeBytes)
	case left.Hash() == nil:
		return nil, errors.Errorf("nil left hash")
	default:
		w(left.Hash().Bytes())
	}

	switch {
	case right == nil:
		w(emptyNodeBytes)
	case right.Hash() == nil:
		return nil, errors.Errorf("nil right hash")
	default:
		w(right.Hash().Bytes())
	}

	return sum(), nil
}

func IsEqualNode(a, b Node) bool {
	switch {
	case a == nil && b == nil:
		return true
	case a == nil, b == nil:
		return false
	}

	switch {
	case a.Key() != b.Key():
		return false
	case a.Hash() == nil || b.Hash() == nil:
		return a.Hash() == nil && b.Hash() == nil
	case !a.Hash().Equal(b.Hash()):
		return false
	}

	return true
}
