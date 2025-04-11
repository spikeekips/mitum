package fixedtree

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

var emptyNodeBytes = []byte{0x0}

type Node interface {
	util.IsValider
	Key() string
	Hash() string
	Equal(Node) bool
	SetHash(left, right Node) (string, dutil.ErrorFunc, error)
}

type BaseNode struct {
	h   string
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

func (n *BaseNode) Hash() string {
	if n == nil {
		return ""
	}

	return n.h
}

func (n *BaseNode) UpdateHashFunc(h string) dutil.ErrorFunc {
	return func() error {
		if n.h != "" {
			return errors.Errorf("already hashed")
		}

		n.h = h

		return nil
	}
}

func (n *BaseNode) SetHash(left, right Node) (string, dutil.ErrorFunc, error) {
	switch h, err := MakeBaseNodeHash(n, left, right, func() ([]byte, error) {
		return []byte(n.key), nil
	}); {
	case err != nil:
		return "", nil, err
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
	Hash string `json:"hash,omitempty"`
	Key  string `json:"key,omitempty"`
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

func (n *BaseNode) UnmarshalJSON(b []byte) error {
	var m map[string]any
	if err := util.UnmarshalJSON(b, &m); err != nil {
		return err
	}

	var u *BaseNode
	if err := BaseNodeDecodeJSON(m, &u); err != nil {
		return err
	}

	*n = *u

	return nil
}

func BaseNodeDecodeJSON(m map[string]any, t **BaseNode) error {
	n := new(BaseNode)

	if err := dutil.FuncChain(
		dutil.AssignJSON(m["hash"], &n.h),
		dutil.AssignJSON(m["key"], &n.key),
	); err != nil {
		return err
	}

	if n.key == "" && n.h == "" {
		return nil
	}

	return encoder.Assign(n, t)
}

func IsValidNode(n Node, b []byte) error {
	switch {
	case len(n.Key()) < 1:
		return util.ErrInvalid.Errorf("empty key")
	case n.Hash() == "":
		return util.ErrInvalid.Errorf("empty node hash")
	default:
		return nil
	}
}

func MakeBaseNodeHash(self, left, right Node, nodeHashBytes func() ([]byte, error)) (string, error) {
	if self.Key() == "" {
		return "", errors.Errorf("empty key")
	}

	w, sum := dhash.NewHex64Writer()
	defer sum()

	switch b, err := nodeHashBytes(); {
	case err != nil:
		return "", err
	default:
		w(b)
	}

	switch {
	case left == nil:
		w(emptyNodeBytes)
	case left.Hash() == "":
		return "", errors.Errorf("empty left hash")
	default:
		w([]byte(left.Hash()))
	}

	switch {
	case right == nil:
		w(emptyNodeBytes)
	case right.Hash() == "":
		return "", errors.Errorf("empty right hash")
	default:
		w([]byte(right.Hash()))
	}

	return sum().String(), nil
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
	default:
		return a.Hash() == b.Hash()
	}
}
