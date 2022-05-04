package tree

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testFixedtreeProof struct {
	suite.Suite
}

func (t *testFixedtreeProof) newTree(n uint64) Fixedtree {
	trg := NewFixedtreeGenerator(n)

	for i := range make([]int, trg.size) {
		n := newDummyNode(uint64(i), util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	return tr
}

func (t *testFixedtreeProof) copypool(pool map[uint64]FixedtreeNode) map[uint64]FixedtreeNode {
	newpool := map[uint64]FixedtreeNode{}
	for k := range pool {
		newpool[k] = pool[k]
	}

	return newpool
}

func (t *testFixedtreeProof) TestExtract() {
	tr := t.newTree(33)

	expected := [][]uint64{
		{1, 2, 0},
		{3, 4, 1, 2, 0},
		{5, 6, 1, 2, 0},
		{7, 8, 3, 4, 1, 2, 0},
		{9, 10, 3, 4, 1, 2, 0},
		{11, 12, 5, 6, 1, 2, 0},
		{13, 14, 5, 6, 1, 2, 0},
		{15, 16, 7, 8, 3, 4, 1, 2, 0},
		{17, 18, 7, 8, 3, 4, 1, 2, 0},
		{19, 20, 9, 10, 3, 4, 1, 2, 0},
		{21, 22, 9, 10, 3, 4, 1, 2, 0},
		{23, 24, 11, 12, 5, 6, 1, 2, 0},
		{25, 26, 11, 12, 5, 6, 1, 2, 0},
		{27, 28, 13, 14, 5, 6, 1, 2, 0},
		{29, 30, 13, 14, 5, 6, 1, 2, 0},
		{31, 32, 15, 16, 7, 8, 3, 4, 1, 2, 0},
		{15, 16, 7, 8, 3, 4, 1, 2, 0},
		{17, 18, 7, 8, 3, 4, 1, 2, 0},
		{17, 18, 7, 8, 3, 4, 1, 2, 0},
		{19, 20, 9, 10, 3, 4, 1, 2, 0},
		{19, 20, 9, 10, 3, 4, 1, 2, 0},
		{21, 22, 9, 10, 3, 4, 1, 2, 0},
		{21, 22, 9, 10, 3, 4, 1, 2, 0},
		{23, 24, 11, 12, 5, 6, 1, 2, 0},
		{23, 24, 11, 12, 5, 6, 1, 2, 0},
		{25, 26, 11, 12, 5, 6, 1, 2, 0},
		{25, 26, 11, 12, 5, 6, 1, 2, 0},
		{27, 28, 13, 14, 5, 6, 1, 2, 0},
		{27, 28, 13, 14, 5, 6, 1, 2, 0},
		{29, 30, 13, 14, 5, 6, 1, 2, 0},
		{29, 30, 13, 14, 5, 6, 1, 2, 0},
		{31, 32, 15, 16, 7, 8, 3, 4, 1, 2, 0},
		{31, 32, 15, 16, 7, 8, 3, 4, 1, 2, 0},
	}

	for index := uint64(0); index < 33; index++ {
		pool := map[uint64]FixedtreeNode{}
		key, indices, err := ExtractProofMaterialsFromFixedtree(tr, index, pool)
		t.NoError(err)

		proof := NewBaseFixedtreeProof(key, indices, pool)
		t.NoError(proof.IsValid(nil))

		t.Run("key", func() {
			n, err := tr.Node(index)
			t.NoError(err)
			t.Equal(n.Key(), key)
		})

		t.Run("indices", func() {
			t.Equal(expected[index], indices)
		})

		t.Run("duplicated index", func() {
			t.False(util.CheckSliceDuplicated(indices, func(i interface{}) string {
				return fmt.Sprintf("%d", i.(uint64))
			}))
		})

		t.Run("pool", func() {
			t.Equal(len(indices), len(pool))

			for i := range indices {
				index := indices[i]

				r, found := pool[index]
				t.True(found)
				t.NotNil(r)

				n, err := tr.Node(index)
				t.NoError(err)
				t.True(n.Equal(r))
			}
		})
	}
}

func (t *testFixedtreeProof) TestMerge() {
	tr := t.newTree(33)

	var proof BaseFixedtreeProof
	{
		pool := map[uint64]FixedtreeNode{}
		key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 10, pool)
		t.NoError(err)

		proof = NewBaseFixedtreeProof(key, indices, pool)
		t.NoError(proof.IsValid(nil))
	}

	t.Run("merge known", func() {
		pool := map[uint64]FixedtreeNode{}
		key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 10, pool)
		t.NoError(err)

		_, err = proof.Merge(key, indices, pool)
		t.Error(err)
		t.ErrorContains(err, "already merged")
	})

	t.Run("merge new", func() {
		pool := map[uint64]FixedtreeNode{}
		key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 3, pool)
		t.NoError(err)

		nproof, err := proof.Merge(key, indices, pool)
		t.NoError(err)

		t.NoError(nproof.IsValid(nil))

		t.Equal(2, len(nproof.indices))

		t.Equal([]string{proof.Keys()[0], key}, nproof.Keys())
		t.Equal(proof.indices[0], nproof.indices[0])
		t.Equal(indices, nproof.indices[1])

		var i int
	end:
		for {
			var index uint64
			switch {
			case i < len(nproof.indices[0]):
				index = nproof.indices[0][i]
			case i >= len(nproof.pool):
				break end
			default:
				index = nproof.indices[1][i-len(nproof.indices[0])]
			}
			i++

			n, found := nproof.pool[index]
			t.True(found)
			t.Equal(index, n.Index())
		}
	})

	t.Run("wrong root hash", func() {
		pool := map[uint64]FixedtreeNode{}
		key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 3, pool)
		t.NoError(err)

		pool[0] = newDummyNode(0, util.UUID().String())
		_, err = proof.Merge(key, indices, pool)
		t.Error(err)
		t.ErrorContains(err, "root hash does not match")
	})
}

func (t *testFixedtreeProof) TestInvalid() {
	tr := t.newTree(33)

	pool := map[uint64]FixedtreeNode{}
	key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 10, pool)
	t.NoError(err)

	var proof BaseFixedtreeProof
	t.Run("valid", func() {
		proof = NewBaseFixedtreeProof(key, indices, pool)
		t.NoError(proof.IsValid(nil))
	})

	t.Run("empty keys", func() {
		proof := proof
		proof.keys = nil

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty keys")
	})

	t.Run("empty indices", func() {
		proof := proof
		proof.indices = nil

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty main indices")
	})

	t.Run("empty pool", func() {
		proof := proof
		proof.pool = nil

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty pool")
	})

	t.Run("len(keys) != len(indices)", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "wrong keys and main indices")
	})

	t.Run("duplicated keys", func() {
		proof := proof
		proof.keys = append(proof.keys, proof.keys[0])
		proof.indices = append(proof.indices, proof.indices[0])

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "duplicated keys found")
	})

	t.Run("duplicated main indices", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")
		proof.indices = append(proof.indices, proof.indices[0])

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "duplicated main indices found")
	})

	t.Run("empty key", func() {
		proof := proof
		proof.keys = append(proof.keys, "")
		proof.indices = append(proof.indices, []uint64{3, 2, 1, 0})

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty key found")
	})

	t.Run("empty indices", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")
		proof.indices = append(proof.indices, []uint64{})

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "empty indices found")
	})

	t.Run("none-zero ended indices", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")
		proof.indices = append(proof.indices, []uint64{1, 2, 3})

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "last indices item must be 0")
	})

	t.Run("wrong length indices", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")
		proof.indices = append(proof.indices, []uint64{1, 1, 3, 0})

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "wrong indices length")
	})

	t.Run("duplicated index", func() {
		proof := proof
		proof.keys = append(proof.keys, "showme")
		proof.indices = append(proof.indices, []uint64{1, 1, 3, 4, 0})

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "duplicated index found")
	})

	t.Run("index not found in pool", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))
		delete(proof.pool, proof.indices[0][0])

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "index not found in pool")
	})

	t.Run("index not match in pool", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))

		index := proof.indices[0][0]
		proof.pool[index] = newDummyNode(index+1, util.UUID().String())

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "index does not match with pool node")
	})

	t.Run("invalid node in pool", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))

		index := proof.indices[0][0]
		proof.pool[index] = newDummyNode(index, "").SetHash(util.UUID().Bytes())

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "invalid pool node")
	})

	t.Run("key node not found in pool", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))

		var index uint64
		for k := range proof.pool {
			if proof.pool[k].Key() == proof.Keys()[0] {
				index = k

				break
			}
		}

		proof.pool[index] = newDummyNode(index, util.UUID().String()).SetHash(util.UUID().Bytes())

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "key node not found in pool")
	})

	t.Run("wrong pool", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))

		proof.pool[100] = newDummyNode(100, util.UUID().String()).SetHash(util.UUID().Bytes())

		err := proof.IsValid(nil)
		t.Error(err)
		t.True(errors.Is(err, util.InvalidError))
		t.ErrorContains(err, "wrong pool")
	})
}

func (t *testFixedtreeProof) TestProve() {
	tr := t.newTree(33)

	pool := map[uint64]FixedtreeNode{}
	key, indices, err := ExtractProofMaterialsFromFixedtree(tr, 10, pool)
	t.NoError(err)

	proof := NewBaseFixedtreeProof(key, indices, pool)
	t.NoError(proof.IsValid(nil))

	keys := proof.Keys()
	for i := range keys {
		key := keys[i]
		t.NoError(proof.Prove(key))
	}

	t.Run("unknown key", func() {
		err := proof.Prove(util.UUID().String())
		t.Error(err)
		t.ErrorContains(err, "key not found")
	})

	t.Run("wrong indices", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))
		proof.indices = [][]uint64{{1, 2, 3, 4}}

		err := proof.Prove(proof.Keys()[0])
		t.Error(err)
		t.ErrorContains(err, "key does not match")
	})

	t.Run("node not found", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))
		delete(proof.pool, proof.indices[0][0])

		err := proof.Prove(proof.Keys()[0])
		t.Error(err)
		t.ErrorContains(err, "node not found in pool")
	})

	t.Run("wrong hash", func() {
		proof := NewBaseFixedtreeProof(key, indices, t.copypool(pool))
		proof.pool[0] = proof.pool[0].SetHash(util.UUID().Bytes())

		err := proof.Prove(proof.Keys()[0])
		t.Error(err)
		t.ErrorContains(err, "node, 0 has wrong hash")
	})
}

func TestFixedtreeProof(t *testing.T) {
	suite.Run(t, new(testFixedtreeProof))
}
