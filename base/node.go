package base

import (
	"sort"
	"strings"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type Node interface {
	hint.Hinter
	util.HashByter
	util.IsValider
	Address() Address
	Publickey() Publickey
}

type LocalNode interface {
	Node
	Privatekey() Privatekey
}

func DecodeNode(b []byte, enc encoder.Encoder) (Node, error) {
	e := util.StringErrorFunc("failed to decode node")

	hinter, err := enc.Decode(b)
	if err != nil {
		return nil, e(err, "")
	}

	i, ok := hinter.(Node)
	if !ok {
		return nil, e(nil, "expected Node, but %T", hinter)
	}

	return i, nil
}

func IsEqualNode(a, b Node) bool {
	switch {
	case a == nil || b == nil:
		return false
	case !a.Address().Equal(b.Address()):
		return false
	case !a.Publickey().Equal(b.Publickey()):
		return false
	default:
		return true
	}
}

func IsEqualNodes(a, b []Node) bool {
	switch {
	case len(a) < 1 && len(b) < 1:
		return true
	case len(a) != len(b):
		return false
	}

	sort.Slice(a, func(i, j int) bool {
		return strings.Compare(
			a[i].Address().String(),
			a[j].Address().String(),
		) < 0
	})

	sort.Slice(b, func(i, j int) bool {
		return strings.Compare(
			b[i].Address().String(),
			b[j].Address().String(),
		) < 0
	})

	for i := range a {
		if !IsEqualNode(a[i], b[i]) {
			return false
		}
	}

	return true
}
