package tree

import (
	"bytes"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Proof interface {
	util.IsValider
	Keys() []string // NOTE []<node key>
	Prove(key string, info ...interface{}) error
}

type BaseFixedtreeProof struct {
	keys    []string
	indices [][]uint64 /* index */
	pool    map[ /* index */ uint64]FixedtreeNode
}

func NewBaseFixedtreeProof(key string, indices []uint64, pool map[uint64]FixedtreeNode) BaseFixedtreeProof {
	return BaseFixedtreeProof{
		keys:    []string{key},
		indices: [][]uint64{indices},
		pool:    pool,
	}
}

func (p BaseFixedtreeProof) IsValid(b []byte) error {
	e := util.StringErrorFunc("invalid BaseFixedtreeProof")

	if err := p.isValidCommon(b); err != nil {
		return e(util.InvalidError.Wrap(err), "")
	}

	if err := p.isValidPool(b); err != nil {
		return e(util.InvalidError.Wrap(err), "")
	}

	return nil
}

func (p BaseFixedtreeProof) isValidCommon([]byte) error {
	switch {
	case len(p.keys) < 1:
		return errors.Errorf("empty keys")
	case len(p.indices) < 1:
		return errors.Errorf("empty main indices")
	case len(p.pool) < 1:
		return errors.Errorf("empty pool")
	case len(p.keys) != len(p.indices):
		return errors.Errorf("wrong keys and main indices")
	case util.CheckSliceDuplicated(p.keys, func(i interface{}) string { return i.(string) }):
		return errors.Errorf("duplicated keys found")
	case util.CheckSliceDuplicated(p.indices, func(i interface{}) string { return fmt.Sprintf("%v", i) }):
		return errors.Errorf("duplicated main indices found")
	}

	for i := range p.keys {
		if len(p.keys[i]) < 1 {
			return errors.Errorf("empty key found")
		}
	}

	for i := range p.indices {
		switch indices := p.indices[i]; {
		case len(indices) < 3:
			return errors.Errorf("empty indices found")
		case indices[len(indices)-1] != 0:
			return errors.Errorf("last indices item must be 0")
		case len(indices)%2 != 1:
			return errors.Errorf("wrong indices length")
		case util.CheckSliceDuplicated(indices, func(i interface{}) string { return fmt.Sprintf("%d", i) }):
			return errors.Errorf("duplicated index found")
		}
	}

	return nil
}

func (p BaseFixedtreeProof) isValidPool(b []byte) error {
	allindices := map[uint64]struct{}{}
	for i := range p.indices {
		var foundkey bool
		for j := range p.indices[i] {
			index := p.indices[i][j]

			switch n, found := p.pool[index]; {
			case !found:
				return errors.Errorf("index not found in pool")
			case n.Index() != index:
				return errors.Errorf("index does not match with pool node")
			case n.Key() == p.keys[i]:
				foundkey = true
			default:
				if err := n.IsValid(b); err != nil {
					return errors.Errorf("invalid pool node")
				}
			}

			allindices[index] = struct{}{}
		}

		if !foundkey {
			return errors.Errorf("key node not found in pool")
		}
	}

	if len(allindices) != len(p.pool) {
		return errors.Errorf("wrong pool")
	}

	return nil
}

func (p BaseFixedtreeProof) Keys() []string {
	return p.keys
}

func (p BaseFixedtreeProof) Prove(key string, _ ...interface{}) error {
	var indices []uint64
	for i := range p.keys {
		if p.keys[i] == key {
			indices = p.indices[i]

			break
		}
	}

	e := util.StringErrorFunc("failed to prove BaseFixedtreeProof")

	if len(indices) < 1 {
		return e(nil, "key not found")
	}

	for i := 0; i < len(indices[:len(indices)-1])/2; i++ {
		switch parent, err := p.proveByHeight(i*2, indices); {
		case err != nil:
			return e(err, "")
		case i == 0 && parent.Key() != key:
			return e(nil, "key does not match")
		}
	}

	return nil
}

func (p BaseFixedtreeProof) proveByHeight(h int, indices []uint64) (parent FixedtreeNode, err error) {
	var a, b FixedtreeNode
	switch i, found := p.pool[indices[h]]; {
	case !found:
		return nil, errors.Errorf("node not found in pool")
	default:
		a = i
	}

	switch i, found := p.pool[indices[h+1]]; {
	case !found:
		return nil, errors.Errorf("node not found in pool")
	default:
		b = i
	}

	switch i, err := parentFixedtree(a.Index()); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		switch n, found := p.pool[i]; {
		case !found:
			return nil, errors.Errorf("node not found in pool")
		default:
			parent = n
		}
	}

	switch h, err := fixedtreeNodeHash(parent, a, b); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	case !bytes.Equal(parent.Hash(), h):
		return nil, errors.Errorf("node, %d has wrong hash", parent.Index())
	default:
		return parent, nil
	}
}

// Merge merges new proof material to exiting. new proof material should be
// valid.
func (p BaseFixedtreeProof) Merge(
	key string,
	indices []uint64,
	pool map[uint64]FixedtreeNode,
) (proof BaseFixedtreeProof, err error) {
	e := util.StringErrorFunc("failed to merge BaseFixedtreeProof")

	switch {
	case len(p.keys) < 1:
		return NewBaseFixedtreeProof(key, indices, pool), nil
	case !bytes.Equal(pool[0].Hash(), p.pool[0].Hash()):
		return proof, e(nil, "root hash does not match")
	}

	if util.InStringSlice(key, p.keys) {
		return proof, e(nil, "already merged")
	}

	p.keys = append(p.keys, key)
	p.indices = append(p.indices, indices)

	for k := range pool {
		if _, found := p.pool[k]; found {
			continue
		}

		p.pool[k] = pool[k]
	}

	return p, nil
}

func ExtractProofMaterialsFromFixedtree(
	tr Fixedtree,
	index uint64,
	pool map[uint64]FixedtreeNode,
) (key string, indices []uint64, err error) {
	e := util.StringErrorFunc("failed to make proof")

	if tr.Len() < 1 {
		return "", nil, e(nil, "empty tree")
	}

	switch n, err := tr.Node(index); {
	case err != nil:
		return "", nil, e(err, "")
	default:
		key = n.Key()
	}

	height := heightFixedtree(index)

	insertPool := func(i uint64) {
		if _, found := pool[i]; found {
			return
		}

		pool[i] = tr.nodes[i]
	}

	indices = make([]uint64, (height+1)*2+1)
	l := index
end:
	for i := 0; ; {
		switch k, err := childrenFixedtree(tr.Len(), l); {
		case err == nil:
			indices[(i * 2)] = k[0]
			indices[(i*2)+1] = k[1]

			insertPool(k[0])
			insertPool(k[1])

			i++
		case errors.Is(err, noChildrenError):
			if l == index {
				indices = make([]uint64, (height)*2+1)
			}
		default:
			return "", nil, e(err, "")
		}

		switch j, err := parentFixedtree(l); {
		case err == nil:
			l = j
		case errors.Is(err, noParentError):
			if len(indices) < 1 {
				indices = []uint64{l}
			}

			insertPool(0)

			break end
		default:
			return "", nil, e(err, "")
		}
	}

	switch {
	case pool[0] == nil:
		return "", nil, e(nil, "empty root item")
	case indices[len(indices)-1] != 0:
		return "", nil, e(nil, "last indices item must be 0")
	}

	return key, indices, nil
}
