package fixedtree

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

var emptyNodeString = "<empty>"

type Node interface {
	util.IsValider
	Key() string
	Hash() util.Hash
	Equal(Node) bool
	IsEmpty() bool
	SetHash(util.Hash) Node
}

type BaseNode struct {
	h       util.Hash
	key     string
	isempty bool
}

func ParseBaseNodeString(s string) (n BaseNode, err error) {
	if s == emptyNodeString {
		return EmptyBaseNode(), nil
	}

	e := util.StringErrorFunc("failed to parse BaseNode")

	l := strings.SplitN(s, " ", 2)

	switch {
	case len(l) != 2: //nolint:gomnd //...
		return n, e(nil, "invalid string")
	case len(l[0]) > 0:
		n.h = valuehash.NewBytesFromString(l[0])
	}

	n.key = l[1]

	return n, nil
}

func NewBaseNode(key string) BaseNode {
	return BaseNode{key: key}
}

func EmptyBaseNode() BaseNode {
	return BaseNode{isempty: true}
}

func (n BaseNode) Key() string {
	return n.key
}

func (n BaseNode) Hash() util.Hash {
	if n.isempty {
		return valuehash.Bytes(nil)
	}

	return n.h
}

func (n BaseNode) SetHash(h util.Hash) Node {
	return BaseNode{h: h, key: n.key, isempty: n.isempty}
}

func (n BaseNode) Equal(b Node) bool {
	if b == nil {
		return false
	}

	switch {
	case n.IsEmpty() || b.IsEmpty():
		return n.IsEmpty() && b.IsEmpty()
	case n.Key() != b.Key():
		return false
	case n.Hash() == nil || b.Hash() == nil:
		return !(n.Hash() != nil || b.Hash() != nil)
	case !n.Hash().Equal(b.Hash()):
		return false
	default:
		return true
	}
}

func (n BaseNode) IsEmpty() bool {
	return n.isempty
}

func (n BaseNode) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid Node")

	switch {
	case n.isempty:
		return nil
	case len(n.key) < 1:
		return e.Errorf("empty key")
	case n.h == nil:
		return e.Errorf("empty hash")
	}

	if err := n.h.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (n BaseNode) String() string {
	switch {
	case n.isempty:
		return emptyNodeString
	default:
		return fmt.Sprintf("%s %s", n.h.String(), n.key)
	}
}

type BaseNodeJSONMarshaler struct {
	Hash    util.Hash `json:"hash,omitempty"`
	Key     string    `json:"key,omitempty"`
	Isempty bool      `json:"isempty,omitempty"`
}

func (n BaseNode) JSONMarshaler() BaseNodeJSONMarshaler {
	if n.isempty {
		return BaseNodeJSONMarshaler{
			Isempty: n.isempty,
		}
	}

	return BaseNodeJSONMarshaler{
		Key:  n.key,
		Hash: n.h,
	}
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(n.JSONMarshaler())
}

type baseNodeJSONUnmarshaler struct {
	Hash    valuehash.HashDecoder `json:"hash"`
	Key     string                `json:"key"`
	Isempty bool                  `json:"isempty"`
}

func (n *BaseNode) UnmarshalJSON(b []byte) error {
	var u baseNodeJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.WithMessage(err, "failed to unmarshal BaseNode")
	}

	if u.Isempty {
		n.key = ""
		n.h = nil
		n.isempty = true

		return nil
	}

	n.key = u.Key
	n.h = u.Hash.Hash()

	return nil
}
