package tree

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/stretchr/testify/suite"
)

var dummyNodeHint = hint.MustNewHint("tree-node-v0.0.1")

type dummyNode struct {
	BaseFixedtreeNode
}

func newDummyNode(index uint64, key string) dummyNode {
	return dummyNode{
		BaseFixedtreeNode: NewBaseFixedtreeNode(dummyNodeHint, index, key),
	}
}

func newDummyNodeWithHash(index uint64, key string, h []byte) dummyNode {
	return dummyNode{
		BaseFixedtreeNode: NewBaseFixedtreeNodeWithHash(dummyNodeHint, index, key, h),
	}
}

type testFixedtreeNode struct {
	suite.Suite
}

func (t *testFixedtreeNode) TestEmptyKey() {
	err := newDummyNode(1, "").IsValid(nil)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "empty key")
}

func (t *testFixedtreeNode) TestEmptyHash() {
	err := newDummyNode(1, util.UUID().String()).IsValid(nil)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "empty hash")
}

func (t *testFixedtreeNode) TestEncodeJSON() {
	no := newDummyNodeWithHash(20, util.UUID().String(), util.UUID().Bytes())

	b, err := util.MarshalJSON(&no)
	t.NoError(err)
	t.NotNil(b)

	var uno BaseFixedtreeNode
	t.NoError(util.UnmarshalJSON(b, &uno))

	t.True(no.Equal(uno))
}

func TestFixedtreeNode(t *testing.T) {
	suite.Run(t, new(testFixedtreeNode))
}

type testFixedtree struct {
	suite.Suite
}

func (t *testFixedtree) TestWrongHash() {
	trg := NewFixedtreeGenerator(3)

	t.NoError(trg.Add(newDummyNode(0, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(1, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(2, util.UUID().String())))

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	tr.nodes[2] = tr.nodes[2].SetHash([]byte("showme"))
	err = tr.IsValid(nil)
	t.True(errors.Is(err, util.InvalidError))
	t.ErrorContains(err, "wrong node hash")
}

func (t *testFixedtree) TestTraverse() {
	trg := NewFixedtreeGenerator(10)

	for i := 0; i < 10; i++ {
		n := newDummyNode(uint64(i), util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	var i uint64
	t.NoError(tr.Traverse(func(n FixedtreeNode) (bool, error) {
		t.True(n.Equal(tr.nodes[i]))
		i++

		return true, nil
	}))
}

func (t *testFixedtree) TestProof1Index() {
	trg := NewFixedtreeGenerator(10)

	for i := 0; i < 10; i++ {
		n := newDummyNode(uint64(i), util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	pr, err := tr.Proof(1)
	t.NoError(err)

	t.NoError(ProveFixedtreeProof(pr))
}

func (t *testFixedtree) TestProof0Index() {
	trg := NewFixedtreeGenerator(10)

	for i := 0; i < 10; i++ {
		n := newDummyNode(uint64(i), util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	pr, err := tr.Proof(0)
	t.NoError(err)

	t.NoError(ProveFixedtreeProof(pr))
}

func (t *testFixedtree) TestProofWrongSelfHash() {
	l := uint64(15)
	trg := NewFixedtreeGenerator(l)

	for i := uint64(0); i < l; i++ {
		n := newDummyNode(i, util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	pr, err := tr.Proof(4)
	t.NoError(err)

	pr[0] = pr[0].SetHash(util.UUID().Bytes()) // NOTE make wrong hash

	err = ProveFixedtreeProof(pr)
	t.True(errors.Is(err, InvalidProofError))
	t.ErrorContains(err, "wrong hash")
}

func (t *testFixedtree) TestProofWrongHash() {
	l := uint64(15)
	trg := NewFixedtreeGenerator(l)

	for i := uint64(0); i < l; i++ {
		n := newDummyNode(i, util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	pr, err := tr.Proof(4)
	t.NoError(err)

	n := pr[3].(BaseFixedtreeNode)
	n.key = util.UUID().String() // NOTE make wrong key
	pr[3] = n

	err = ProveFixedtreeProof(pr)
	t.True(errors.Is(err, InvalidProofError))
	t.ErrorContains(err, "wrong hash")
}

func (t *testFixedtree) TestProof() {
	l := uint64(15)
	trg := NewFixedtreeGenerator(l)

	for i := uint64(0); i < l; i++ {
		n := newDummyNode(i, util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	pr, err := tr.Proof(4)
	t.NoError(err)

	t.NoError(ProveFixedtreeProof(pr))
}

func (t *testFixedtree) TestEncodeJSON() {
	l := uint64(15)
	trg := NewFixedtreeGenerator(l)

	for i := uint64(0); i < l; i++ {
		n := newDummyNode(i, util.UUID().String())
		t.NoError(trg.Add(n))
	}

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	b, err := util.MarshalJSON(&tr)
	t.NoError(err)

	enc := jsonenc.NewEncoder()
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: dummyNodeHint, Instance: dummyNode{}}))
	t.NoError(enc.Add(encoder.DecodeDetail{Hint: FixedtreeHint, Instance: Fixedtree{}}))

	hinter, err := enc.Decode(b)
	t.NoError(err)

	utr := hinter.(Fixedtree)

	t.Equal(tr.Len(), utr.Len())

	t.NoError(tr.Traverse(func(n FixedtreeNode) (bool, error) {
		if i, err := utr.Node(n.Index()); err != nil {
			return false, err
		} else if !n.Equal(i) {
			return false, errors.Errorf("not equal")
		}

		return true, nil
	}))
}

func TestFixedtree(t *testing.T) {
	suite.Run(t, new(testFixedtree))
}

type testFixedtreeGenerator struct {
	suite.Suite
}

func (t *testFixedtreeGenerator) TestNew() {
	trg := NewFixedtreeGenerator(10)
	t.NotNil(trg)
	t.Equal(10, len(trg.nodes))

	trg = NewFixedtreeGenerator(9)
	t.NotNil(trg)
	t.Equal(9, len(trg.nodes))
}

func (t *testFixedtreeGenerator) TestZeroSize() {
	trg := NewFixedtreeGenerator(0)
	t.NotNil(trg)
	t.Equal(0, len(trg.nodes))
}

func (t *testFixedtreeGenerator) TestAddOutOfRange() {
	trg := NewFixedtreeGenerator(3)

	t.NoError(trg.Add(newDummyNode(1, util.UUID().String())))

	err := trg.Add(newDummyNode(3, util.UUID().String()))
	t.ErrorContains(err, "out of range")
}

func (t *testFixedtreeGenerator) TestAddSetNilHash() {
	trg := NewFixedtreeGenerator(3)

	n := newDummyNode(1, util.UUID().String())
	n.hash = util.UUID().Bytes()

	t.NoError(trg.Add(n))
	t.Nil(trg.nodes[1].Hash())
}

func (t *testFixedtreeGenerator) TestTreeNotFilled() {
	trg := NewFixedtreeGenerator(3)

	t.NoError(trg.Add(newDummyNode(0, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(2, util.UUID().String())))

	_, err := trg.Tree()
	t.ErrorContains(err, "empty node")
}

func (t *testFixedtreeGenerator) TestTreeFilled() {
	trg := NewFixedtreeGenerator(3)

	t.NoError(trg.Add(newDummyNode(0, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(1, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(2, util.UUID().String())))

	tr, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))
}

func (t *testFixedtreeGenerator) TestTreeAgain() {
	trg := NewFixedtreeGenerator(3)

	t.NoError(trg.Add(newDummyNode(0, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(1, util.UUID().String())))
	t.NoError(trg.Add(newDummyNode(2, util.UUID().String())))

	tr0, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr0.IsValid(nil))

	tr1, err := trg.Tree()
	t.NoError(err)
	t.NoError(tr1.IsValid(nil))

	for i := range tr0.nodes {
		a := tr0.nodes[i]
		b := tr1.nodes[i]

		t.True(a.Equal(b), "index=%d", i)
	}
}

func (t *testFixedtreeGenerator) TestNodeHash() {
	trg := NewFixedtreeGenerator(20)

	for i := 0; i < 20; i++ {
		b := fmt.Sprintf("%d", i)
		t.NoError(trg.Add(newDummyNode(uint64(i), b)))
	}

	expectedHashes := []string{
		"EQCKyWqfF3EG7d9aNEwf9ZEGNnimYsvTjSRmUuEKfqbY",
		"8Dtg6sPXM8GpwF1SeR6YU3rZRryT6ri1Hh6CgHxHrSvx",
		"HxM1urjQdYUyjpXzwG6hrvkoFNp2e1gG89r6Yhjcrdsb",
		"Bi9s2jPt24GT2WQrNV78XdeUHpDUuytQQ26zpZsbyYvC",
		"rtpHg163dSBi2g48xCdXeEqvudBhswufZZ3gBpJNZha",
		"iAhp5H7h5gzmVBTNrvhxUaPtQ57whY8sadSPodhc2y9",
		"3hUZg43jgZKVL8LmbCi8AsiytJeeDUFR5iRWor9FDJXA",
		"6Lp4VVAhXJrYGmNd4KroDiXKYbbL65dqB83xWdhfWxXR",
		"DdcUJdxWJGH6jv1chSpPChesFNSFEPH3prsHyfdKEUJa",
		"FQD7GAFiVC3Nb5nkdXh9bhQCkJHasXBmLPtave7aduhU",
		"9E11xW24jYk4aioUsBesSRWqt7iryHnjyn8VdV3bjseu",
		"ACz9RrSa2ktpNaMWuvrT9pCQKWGa6txnSREDZKD7V3Li",
		"4R91rUkdKxa5XAY5r6TdJW79V7XhYC27i8skuT5yyn9W",
		"FnZJd4FdURCuFfrvTGawTBmi99yBJb4UMHDFuGNhmpGp",
		"uSCJRdChaDrEGFYdiTD9zCtEkFmj1iPrapKyu2rJbCP",
		"7XmBvBXgLFp99Py6nLECYF9JqToR71KLaNSowqRZEEB6",
		"7eNhEDpVW4BmBvgXxYrnSFF6JVejTVVs8Yc6qkm4uBF4",
		"FEt7r23RgYTmT7o4bBGvTxTKTbpRCYcqpgyasxneKpb1",
		"5opVDS3QcC5HUGJcqstwuALNoaRS2MPSN5ewbN8LqYWN",
		"BAPXwD6pSwxfZvmWE7jHMFKYSQkFPcBXDfLAJjRoQJGV",
	}

	tr, err := trg.Tree()
	t.NoError(err)
	for i := range tr.nodes {
		t.Equal(expectedHashes[i], base58.Encode(tr.nodes[i].Hash()))
	}
}

func (t *testFixedtreeGenerator) TestAddMany() {
	var size uint64 = 200000
	var root []byte
	{
		tr := NewFixedtreeGenerator(size)

		s := time.Now()
		for i := uint64(0); i < tr.size; i++ {
			t.NoError(tr.Add(newDummyNode(i, fmt.Sprintf("%d", i))))
		}
		t.T().Log("from root:  insert: elapsed", tr.size, time.Since(s))

		s = time.Now()
		root = tr.Root()
		t.T().Log("from root: hashing: elapsed", tr.size, time.Since(s))
	}

	{
		tr := NewFixedtreeGenerator(size)

		s := time.Now()
		for i := uint64(0); i < tr.size; i++ {
			j := tr.size - 1 - i
			t.NoError(tr.Add(newDummyNode(j, fmt.Sprintf("%d", j))))
		}
		t.T().Log(" from end:  insert: elapsed", tr.size, time.Since(s))

		s = time.Now()
		root0 := tr.Root()
		t.T().Log(" from end: hashing: elapsed", tr.size, time.Since(s))

		t.Equal(root, root0)
	}
}

func (t *testFixedtreeGenerator) TestParallel() {
	var size uint64 = 200000

	var root []byte
	{
		tr := NewFixedtreeGenerator(size)

		s := time.Now()
		for i := uint64(0); i < tr.size; i++ {
			t.NoError(tr.Add(newDummyNode(i, fmt.Sprintf("%d", i))))
		}
		t.T().Log("     add:  insert: elapsed", tr.size, time.Since(s))

		s = time.Now()
		root = tr.Root()
		t.T().Log("     add: hashing: elapsed", tr.size, time.Since(s))
	}

	{
		l := make([]uint64, size)
		for i := uint64(0); i < size; i++ {
			l[i] = i
		}

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(l), func(i, j int) { l[i], l[j] = l[j], l[i] })

		tr := NewFixedtreeGenerator(size)

		indexChan := make(chan uint64, size)
		done := make(chan struct{}, size)
		s := time.Now()

		for i := 0; i < 10; i++ {
			i := i
			go func() {
				for j := range indexChan {
					t.NoError(tr.Add(newDummyNode(j, fmt.Sprintf("%d", i))))
					done <- struct{}{}
				}
			}()
		}

		go func() {
			for _, i := range l {
				indexChan <- i
			}
			close(indexChan)
		}()

		var count uint64

	end:
		for range done {
			count++
			if count >= size {
				break end
			}
		}

		t.T().Log("parallel:  insert: elapsed", tr.size, time.Since(s))

		s = time.Now()
		root0 := tr.Root()
		t.T().Log("parallel: hashing: elapsed", tr.size, time.Since(s))

		t.Equal(root, root0)
	}
}

func TestFixedtreeGenerator(t *testing.T) {
	suite.Run(t, new(testFixedtreeGenerator))
}
