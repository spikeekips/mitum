package fixedtree

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

type Proof struct {
	nodes []Node
}

func NewProof(extracted []Node) Proof {
	return Proof{nodes: extracted}
}

func NewProofFromNodes(nodes []Node, key string) (p Proof, err error) {
	extracted, err := ExtractProofMaterial(nodes, key)
	if err != nil {
		return p, errors.Wrap(err, "failed to ne wproof from nodes")
	}

	return NewProof(extracted), nil
}

func (p Proof) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid Proof")

	switch n := len(p.nodes); {
	case n < 1:
		return e(util.InvalidError.Errorf("empty extracted"), "")
	case n%2 != 1:
		return e(util.InvalidError.Errorf("invalid number of extracted"), "")
	}

end:
	for i := range p.nodes {
		n := p.nodes[i]

		switch {
		case i > 2 && n == nil:
			return e(util.InvalidError.Errorf("empty node found at %d", i), "")
		case n == nil:
			continue end
		}

		if err := n.IsValid(b); err != nil {
			return e(err, "")
		}
	}

	switch {
	case util.CheckSliceDuplicated(p.nodes, func(i interface{}) string {
		n := i.(Node)
		if n.IsEmpty() {
			return util.UUID().String()
		}

		return n.Key()
	}):
		return errors.Errorf("duplicated key found")
	case util.CheckSliceDuplicated(p.nodes, func(i interface{}) string {
		n := i.(Node)
		if n.IsEmpty() {
			return valuehash.Bytes(util.UUID().Bytes()).String()
		}

		return n.Hash().String()
	}):
		return errors.Errorf("duplicated hash found")
	}

	return nil
}

func (p Proof) Nodes() []Node {
	return p.nodes
}

func (p Proof) Prove(key string) error {
	e := util.StringErrorFunc("failed to prove")

	nodes := p.filterNodes(key)

	if len(nodes) < 1 {
		return e(nil, "key not found")
	}

	for i := 0; i < (len(nodes)-1)/2; i++ {
		bi := i * 2
		parents := nodes[bi+2 : bi+3]
		if i*2+4 < len(nodes) {
			parents = nodes[bi+2 : bi+4]
		}

		var passed bool
	passend:
		for j := range parents {
			if parents[j].IsEmpty() {
				continue
			}

			switch h, err := nodeHash(parents[j], nodes[bi], nodes[bi+1]); {
			case err != nil:
				return e(err, "")
			case parents[j].Hash().Equal(h):
				passed = true

				break passend
			}
		}

		if !passed {
			return e(nil, "hash does not match")
		}
	}

	return nil
}

func (p Proof) filterNodes(key string) (nodes []Node) {
	for i := range p.nodes {
		n := p.nodes[i]
		if n == nil {
			continue
		}

		if n.Key() != key {
			continue
		}

		switch {
		case i%2 == 0:
			nodes = make([]Node, 2+len(p.nodes[i:]))
			if i > 1 {
				copy(nodes[:2], p.nodes[i-2:i])
			}

			copy(nodes[2:], p.nodes[i:])
		case i+1 == len(p.nodes):
			nodes = make([]Node, 3)
			if i > 1 {
				copy(nodes[:2], p.nodes[i-2:i])
			}

			nodes[2] = p.nodes[i]
		default:
			nodes = make([]Node, 2+len(p.nodes[i-1:]))
			if i > 1 {
				copy(nodes[:2], p.nodes[i-3:i-1])
			}

			copy(nodes[2:], p.nodes[i-1:])
		}

		break
	}

	return nodes
}

func ExtractProofMaterial(nodes []Node, key string) (extracted []Node, err error) {
	e := util.StringErrorFunc("failed to make proof material")

	size := len(nodes)
	if size < 1 {
		return nil, e(nil, "empty tree")
	}

	// NOTE find index
	var index uint64
	var node Node
	for i := uint64(0); i < uint64(size); i++ {
		n := nodes[i]
		if n.Key() == key {
			index = i
			node = n

			break
		}

		continue
	}

	if node == nil {
		return nil, e(nil, "key not found")
	}

	height := indexHeight(index)
	extracted = make([]Node, (height+1)*2+1)
	l := index
end:
	for i := 0; ; {
		switch c, err := childrenNodes(nodes, l); {
		case err == nil:
			if i*2 >= 2 && (c[0] == nil && c[1] == nil) {
				return nil, e(nil, "empty children found at %d", l)
			}

			extracted[(i * 2)] = c[0]
			if c[1] == nil {
				c[1] = EmptyBaseNode()
			}
			extracted[(i*2)+1] = c[1]
		case errors.Is(err, noChildrenError):
			if l != index {
				return nil, e(err, "")
			}

			extracted[(i * 2)] = EmptyBaseNode()
			extracted[(i*2)+1] = EmptyBaseNode()
		default:
			return nil, e(err, "")
		}
		i++

		switch j, err := parent(l); {
		case err == nil:
			l = j
		case errors.Is(err, noParentError):
			extracted[len(extracted)-1] = nodes[l]

			break end
		default:
			return nil, e(err, "")
		}
	}

	if extracted[len(extracted)-1] == nil {
		return nil, e(nil, "empty root node found")
	}

	return extracted, nil
}
