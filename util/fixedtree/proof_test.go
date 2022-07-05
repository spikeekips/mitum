package fixedtree

import (
	"testing"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
)

type testProof struct {
	baseTestTree
}

func (t *testProof) TestExtract() {
	nodes := t.nodes(34)

	tr, err := NewTree(t.ht, nodes)
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	expected := [][]int{
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
		{33, -1, 15, 16, 7, 8, 3, 4, 1, 2, 0},
		{-1, -1, 17, 18, 7, 8, 3, 4, 1, 2, 0},
		{-1, -1, 17, 18, 7, 8, 3, 4, 1, 2, 0},
		{-1, -1, 19, 20, 9, 10, 3, 4, 1, 2, 0},
		{-1, -1, 19, 20, 9, 10, 3, 4, 1, 2, 0},
		{-1, -1, 21, 22, 9, 10, 3, 4, 1, 2, 0},
		{-1, -1, 21, 22, 9, 10, 3, 4, 1, 2, 0},
		{-1, -1, 23, 24, 11, 12, 5, 6, 1, 2, 0},
		{-1, -1, 23, 24, 11, 12, 5, 6, 1, 2, 0},
		{-1, -1, 25, 26, 11, 12, 5, 6, 1, 2, 0},
		{-1, -1, 25, 26, 11, 12, 5, 6, 1, 2, 0},
		{-1, -1, 27, 28, 13, 14, 5, 6, 1, 2, 0},
		{-1, -1, 27, 28, 13, 14, 5, 6, 1, 2, 0},
		{-1, -1, 29, 30, 13, 14, 5, 6, 1, 2, 0},
		{-1, -1, 29, 30, 13, 14, 5, 6, 1, 2, 0},
		{-1, -1, 31, 32, 15, 16, 7, 8, 3, 4, 1, 2, 0},
		{-1, -1, 31, 32, 15, 16, 7, 8, 3, 4, 1, 2, 0},
		{-1, -1, 33, -1, 15, 16, 7, 8, 3, 4, 1, 2, 0},
	}

	for index := uint64(0); index < uint64(tr.Len()); index++ {
		key := tr.nodes[index].Key()
		extracted, err := ExtractProofMaterial(tr.nodes, key)
		t.NoError(err)

		if !t.Run("key", func() {
			var foundkey bool

			var lasts []Node
			switch {
			case len(extracted) < 2:
				lasts = extracted
			case len(extracted) < 5:
				lasts = extracted
			default:
				lasts = extracted[:4]
			}
			for i := range lasts {
				n := lasts[i]
				if n == nil {
					continue
				}

				if n.Key() == key {
					foundkey = true
				}
			}
			t.True(foundkey)
		}) {
			break
		}

		if !t.Run("extracted", func() {
			indices := make([]int, len(extracted))
			for i := range extracted {
				if extracted[i].(BaseNode).isempty {
					indices[i] = -1

					continue
				}

				for j := range tr.nodes {
					if tr.nodes[j].Key() == extracted[i].Key() {
						indices[i] = j

						break
					}
				}
			}

			t.Equal(expected[index], indices)
		}) {
			break
		}

		if !t.Run("duplicated node", func() {
			_, found := util.CheckSliceDuplicated(extracted, func(i interface{}, _ int) string {
				n := i.(Node)
				if n.IsEmpty() {
					return util.UUID().String()
				}

				return n.Key()
			})
			t.False(found)
		}) {
			break
		}

		if !t.Run("isvalid node", func() {
			p := NewProof(extracted)
			t.NoError(p.IsValid(nil))
		}) {
			break
		}
	}
}

func (t *testProof) TestExtractCases() {
	t.Run("empty nodes", func() {
		_, err := ExtractProofMaterial(nil, util.UUID().String())
		t.Error(err)
		t.ErrorContains(err, "empty tree")
	})

	t.Run("key not found", func() {
		nodes := t.nodes(8)

		tr, err := NewTree(t.ht, nodes)
		t.NoError(err)
		t.NoError(tr.IsValid(nil))

		_, err = ExtractProofMaterial(tr.nodes, util.UUID().String())
		t.Error(err)
		t.ErrorContains(err, "key not found")
	})

	t.Run("one root node", func() {
		nodes := t.nodes(1)

		tr, err := NewTree(t.ht, nodes)
		t.NoError(err)
		t.NoError(tr.IsValid(nil))

		key := tr.nodes[0].Key()
		extracted, err := ExtractProofMaterial(tr.nodes, key)
		t.NoError(err)

		t.Equal(3, len(extracted))
		t.True(extracted[0].IsEmpty())
		t.True(extracted[1].IsEmpty())
	})

	t.Run("one children", func() {
		nodes := t.nodes(8)

		tr, err := NewTree(t.ht, nodes)
		t.NoError(err)
		t.NoError(tr.IsValid(nil))

		key := tr.nodes[3].Key()
		extracted, err := ExtractProofMaterial(tr.nodes, key)
		t.NoError(err)

		t.Equal(7, len(extracted))
		t.True(extracted[1].IsEmpty())
	})
}

func (t *testProof) TestProveOneNode() {
	nodes := t.nodes(1)

	tr, err := NewTree(t.ht, nodes)
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	t.Run("prove", func() {
		key := nodes[0].Key()
		extracted, err := ExtractProofMaterial(tr.nodes, key)
		t.NoError(err)

		for i := range extracted {
			n := extracted[i]
			t.T().Logf("index=%d isempty=%v key=%q hash=%q", i, n.IsEmpty(), n.Key(), n.Hash())
		}

		p := NewProof(extracted)
		t.NoError(p.IsValid(nil))

		t.NoError(p.Prove(key))
	})
}

func (t *testProof) TestProveMultipleNodes() {
	nodes := t.nodes(34)

	tr, err := NewTree(t.ht, nodes)
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	for i := range nodes {
		i := i
		t.Run("prove", func() {
			key := nodes[i].Key()
			extracted, err := ExtractProofMaterial(tr.nodes, key)
			t.NoError(err)

			p := NewProof(extracted)
			t.NoError(p.IsValid(nil))

			t.NoError(p.Prove(key))
		})
	}

	t.Run("unknown key", func() {
		key := nodes[33].Key() // last one
		extracted, err := ExtractProofMaterial(tr.nodes, key)
		t.NoError(err)

		p := NewProof(extracted)
		t.NoError(p.IsValid(nil))

		err = p.Prove(util.UUID().String())
		t.Error(err)
		t.ErrorContains(err, "key not found")
	})

	key := nodes[33].Key() // last one
	extracted, err := ExtractProofMaterial(tr.nodes, key)
	t.NoError(err)
	for i := range extracted {
		i := i
		if extracted[i].IsEmpty() {
			continue
		}

		t.Run("wrong hash; pick one node", func() {
			if extracted[i] == nil {
				return
			}

			touched := make([]Node, len(extracted))
			copy(touched, extracted)

			node := touched[i].(BaseNode)
			node.h = valuehash.RandomSHA256()

			touched[i] = node

			p := NewProof(touched)
			t.NoError(p.IsValid(nil))

			err = p.Prove(key)
			t.Error(err)
			t.ErrorContains(err, "hash does not match")
		})
	}
}

func (t *testProof) TestEncode() {
	tt := new(encoder.BaseTestEncode)

	enc := jsonenc.NewEncoder()

	tr, err := NewTree(t.ht, t.nodes(34))
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	key := tr.Node(13).Key()
	proof, err := NewProofFromNodes(tr.Nodes(), key)
	t.NoError(err)

	t.NoError(proof.Prove(key))

	tt.Encode = func() (interface{}, []byte) {
		b, err := enc.Marshal(proof)
		t.NoError(err)

		t.T().Log("marshaled:", string(b))

		return proof, b
	}
	tt.Decode = func(b []byte) interface{} {
		var u Proof
		t.NoError(enc.Unmarshal(b, &u))

		return u
	}
	tt.Compare = func(a interface{}, b interface{}) {
		ap := a.(Proof)
		bp := b.(Proof)

		t.NoError(ap.IsValid(nil))
		t.NoError(bp.IsValid(nil))

		t.Equal(len(ap.nodes), len(bp.nodes))
		for i := range ap.nodes {
			t.True(ap.nodes[i].Equal(bp.nodes[i]))
		}
	}

	suite.Run(t.T(), tt)
}

func TestProof(t *testing.T) {
	suite.Run(t, new(testProof))
}
