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
	key     string
	h       util.Hash
	isempty bool
}

func ParseBaseNodeString(s string) (n BaseNode, err error) {
	if s == emptyNodeString {
		return EmptyBaseNode(), nil
	}

	e := util.StringErrorFunc("failed to parse BaseNode")

	l := strings.SplitN(s, " ", 2)
	switch {
	case len(l) != 2:
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
	n.h = h

	return n
}

func (n BaseNode) Equal(b Node) bool {
	if b == nil {
		return false
	}

	bn, ok := b.(BaseNode)
	if !ok {
		return false
	}

	switch {
	case n.isempty || bn.isempty:
		return false
	case n.key != bn.key:
		return false
	case n.h == nil || bn.h == nil:
		return !(n.h != nil || bn.h != nil)
	case !n.h.Equal(bn.h):
		return false
	default:
		return true
	}
}

func (n BaseNode) IsEmpty() bool {
	return n.isempty
}

func (n BaseNode) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid Node")

	switch {
	case n.isempty:
		return nil
	case len(n.key) < 1:
		return e(util.InvalidError.Errorf("empty key"), "")
	case n.h == nil:
		return e(util.InvalidError.Errorf("empty hash"), "")
	}

	if err := n.h.IsValid(nil); err != nil {
		return e(err, "")
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
	K string    `json:"key,omitempty"`
	H util.Hash `json:"hash,omitempty"`
	E bool      `json:"isempty,omitempty"`
}

func (n BaseNode) JSONMarshaler() BaseNodeJSONMarshaler {
	if n.isempty {
		return BaseNodeJSONMarshaler{
			E: n.isempty,
		}
	}

	return BaseNodeJSONMarshaler{
		K: n.key,
		H: n.h,
	}
}

func (n BaseNode) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(n.JSONMarshaler())
}

type baseNodeJSONUnmarshaler struct {
	K string                `json:"key"`
	H valuehash.HashDecoder `json:"hash"`
	E bool                  `json:"isempty"`
}

func (n *BaseNode) UnmarshalJSON(b []byte) error {
	var u baseNodeJSONUnmarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BaseNode")
	}

	if u.E {
		n.key = ""
		n.h = nil
		n.isempty = true

		return nil
	}

	n.key = u.K
	n.h = u.H.Hash()

	return nil
}
