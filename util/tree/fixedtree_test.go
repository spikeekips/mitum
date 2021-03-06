package tree

import (
	"bytes"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/btcsuite/btcutil/base58"
	"github.com/stretchr/testify/suite"
	"golang.org/x/crypto/sha3"

	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
)

type testFixedTree struct {
	suite.Suite
}

func (t *testFixedTree) TestNew() {
	ft := NewFixedTreeGenerator(10, nil)
	t.NotNil(ft)
	t.Equal(30, len(ft.nodes))

	ft = NewFixedTreeGenerator(9, nil)
	t.NotNil(ft)
	t.Equal(27, len(ft.nodes))

	tr, err := ft.Tree()
	t.NoError(err)

	t.Implements((*hint.Hinter)(nil), tr)
}

func (t *testFixedTree) TestIndex() {
	{
		ft := NewFixedTreeGenerator(3, nil)
		t.NotNil(ft)

		t.NoError(ft.Append(nil, nil))
		t.NoError(ft.Append(nil, nil))
		t.NoError(ft.Append(nil, nil))
		err := ft.Append(nil, nil)
		t.Contains(err.Error(), "already filled")
	}

	{
		ft := NewFixedTreeGenerator(3, nil)
		t.NotNil(ft)

		t.NoError(ft.Add(0, nil, nil))
		t.NoError(ft.Add(1, nil, nil))
		t.NoError(ft.Add(2, nil, nil))
		err := ft.Add(3, nil, nil)
		t.Contains(err.Error(), "over size")
	}
}

func (t *testFixedTree) TestHeight() {
	ft := NewFixedTreeGenerator(20, nil)
	t.NotNil(ft)

	t.Equal(0, ft.height(0))
	t.Equal(-1, ft.height(20))
}

func (t *testFixedTree) TestChildren() {
	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	for i := 0; i < ft.size; i++ {
		children := ft.children(i)

		t.T().Log(fmt.Sprintf("index=%d childrena=%v", i, children))
		if children[0] >= 0 {
			t.NotNil(ft.nodes[children[0]*3])
		}
		if children[1] >= 0 {
			t.NotNil(ft.nodes[children[1]*3])
		}
	}
}

func (t *testFixedTree) TestNodeHash() {
	ft := NewFixedTreeGenerator(20, func(b []byte) []byte {
		h := sha3.Sum256(b)
		return h[:]
	})

	for i := 0; i < ft.size; i++ {
		b := []byte(fmt.Sprintf("%d", i))
		extra := []byte(fmt.Sprintf("extra%d", i))
		t.NoError(ft.Add(i, b, extra))
		t.Equal(b, ft.Key(i))
	}

	expected_hashes := []string{
		"BjGge9T1Tiy63SSGK2359PLtHf2DBAKnaJEcirE2QTpz",
		"2SwHDm8yXxJU2vGyBvQy87kXKbo3juhwiGnx4rcphwvT",
		"DHVJt8sMBUmNuxJ95eLpA2JrP5ZHuim13BvBW2hoGn7R",
		"v6M9D2ykyTeLNnT3z1nzVMfUNp6EDWSGBi5wVTxV8QK",
		"VAuYeQbsCQqJX5KLHE3obfVuUW3RMYB3EwNug4ACwvg",
		"wL1GwB2tYM6RU2S67HVXTZZU61GZsU8yqbe2g2W1WnY",
		"4yHMzwBJKE4bYC8QMMTf7NMPV1oj7hFF7mLzrsi5eWVP",
		"Gha4doiXH7BD14LCqzN4xmif7YJmpAjx6aBHSFavB8GS",
		"B3pFhmZTdFudYoQjtyWTDZFVNVt2rC2UC5jHbUTY757e",
		"CkdZFHD4Li1RGErRW6WfqwA97uXfTdj6n8Y8fTCEyKFW",
		"HRqBic6YP1CMiG8equ7c6WkkbaZX6i9Px7VE4AAxDr2V",
		"FgoZ93Udje5XaqBaCVre4SG77tepkJArM6q4ov8t8Q2q",
		"DbnjurUx16zKwFxNh2VrY5QCcrdnKEfT4NjP3og6hB1W",
		"6EQhyeqQjVSGRQGH58TLuuiTTNs6ooRzF8Sfn3WZFEGK",
		"2jbjfN4PhSKMVPuBJbGcsyY2EimX7aFWnYu76RuhHRZ3",
		"5bMKDXSDNUYeizGPk1k4jXhMJViTyHKfd3eG8kH79CTM",
		"C4patNCpFu3wTV22Kqct2MXstu9qjQGmg4xRFQ1ciHjq",
		"EVWoDGw3DSEyojiKLQjNup7bYVpixP4nrn8nPnfc8rxK",
		"99LPbbFP7pnPwzHKDSLkErgu26g4k9RpeMFdTDbrNSGa",
		"3pPNc4S4Up7bpeeX1mmcXLviDmUQewXotFZpGhESMiib",
	}

	_ = ft.Hash(0)
	for i := 0; i < ft.size; i++ {
		t.Equal(expected_hashes[i], base58.Encode(ft.Hash(i)))
	}

	t.Equal(ft.size*3, len(ft.Nodes()))
}

func (t *testFixedTree) TestNilKey() {
	hashFunc := func(b []byte) []byte {
		h := sha3.Sum256(b)

		return h[:]
	}
	ft := NewFixedTreeGenerator(200, hashFunc)

	for i := 0; i < ft.size; i++ {
		var k []byte
		if i == 10 {
			k = []byte("9d8431a2-e16d-4723-b495-1739e26f5f7e")
		}
		t.NoError(ft.Add(i, k, nil))
	}

	expected := make([]string, ft.size)
	for i := 0; i < ft.size; i++ {
		expected[i] = ""
	}

	expected[0] = "EF7dMmEvhbmZettQA5vp615xo7CTNWvkahyhH6yqXCs8"
	expected[1] = "8ps5UDMFzLGVMhF6WQ4VuLJpvSNnBHHRL8sHu2XcdTEF"
	expected[4] = "4ATNEzPN98D3ujSvsmbYViK4raec3rijSufgLFNCiTTu"
	expected[10] = base58.Encode(hashFunc(ft.Key(10)))

	_ = ft.Hash(0)
	for i := 0; i < ft.size; i++ {
		t.Equal(expected[i], base58.Encode(ft.nodes[(i*3)+1]), "index=%d", i)
	}

	t.Equal(ft.size*3, len(ft.Nodes()))
}

func (t *testFixedTree) TestAppend() {
	var size uint = 200000
	var root []byte
	{
		ft := NewFixedTreeGenerator(size, nil)

		s := time.Now()
		for i := 0; i < ft.size; i++ {
			t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
		}
		t.T().Log("from root:  insert: elapsed", ft.size, time.Since(s))

		s = time.Now()
		root = ft.Hash(0)
		t.T().Log("from root: hashing: elapsed", ft.size, time.Since(s))
	}

	{
		ft := NewFixedTreeGenerator(size, nil)

		s := time.Now()
		for i := ft.size - 1; i >= 0; i-- {
			t.NoError(ft.Append([]byte(fmt.Sprintf("%d", i)), nil))
		}
		t.T().Log(" from end:  insert: elapsed", ft.size, time.Since(s))

		s = time.Now()
		root0 := ft.Hash(0)
		t.T().Log(" from end: hashing: elapsed", ft.size, time.Since(s))

		t.Equal(root, root0)
	}
}

func (t *testFixedTree) TestParallel() {
	var size uint = 200000

	var root []byte
	{
		ft := NewFixedTreeGenerator(size, nil)

		s := time.Now()
		for i := 0; i < ft.size; i++ {
			t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
		}
		t.T().Log("     add:  insert: elapsed", ft.size, time.Since(s))

		s = time.Now()
		root = ft.Hash(0)
		t.T().Log("     add: hashing: elapsed", ft.size, time.Since(s))
	}

	{
		l := make([]int, size)
		for i := 0; i < int(size); i++ {
			l[i] = i
		}

		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(l), func(i, j int) { l[i], l[j] = l[j], l[i] })

		ft := NewFixedTreeGenerator(size, nil)

		indexChan := make(chan int, size)
		done := make(chan struct{}, size)
		s := time.Now()

		for i := 0; i < 10; i++ {
			go func() {
				for i := range indexChan {
					t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
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

		var count uint

	end:
		for range done {
			count++
			if count >= size {
				break end
			}
		}

		t.T().Log("parallel:  insert: elapsed", ft.size, time.Since(s))

		s = time.Now()
		root0 := ft.Hash(0)
		t.T().Log("parallel: hashing: elapsed", ft.size, time.Since(s))

		t.Equal(root, root0)
	}
}

func (t *testFixedTree) TestProof() {
	ft := NewFixedTreeGenerator(20, func(b []byte) []byte {
		h := sha3.Sum256(b)
		return h[:]
	})

	for i := 0; i < ft.size; i++ {
		k := fmt.Sprintf("%d", i)
		t.NoError(ft.Add(i, []byte(k), nil))
	}

	_ = ft.Hash(0)

	_, err := ft.Proof(20)
	t.Contains(err.Error(), "over size")

	pr, err := ft.Proof(19)
	t.NoError(err)
	t.Equal(22, len(pr))

	var keys []string
	var hashes [][]byte
	for i := 0; i < len(pr)/2; i++ {
		key := pr[i*2]
		keys = append(keys, string(key))

		h := pr[i*2+1]
		hashes = append(hashes, h)
	}

	t.Equal([]string{"19", "9", "9", "10", "4", "3", "4", "1", "1", "2", "0"}, keys)

	ids := []int{19, 9, 9, 10, 4, 3, 4, 1, 1, 2, 0}
	for i, h := range hashes {
		t.Equal(ft.Hash(ids[i]), h)
	}
}

func (t *testFixedTree) TestProve() {
	{ // empty proof
		err := ProveFixedTreeProof(nil, nil)
		t.Contains(err.Error(), "nothing to prove")
	}

	{ // wrong sized proof
		err := ProveFixedTreeProof([][]byte{nil}, nil)
		t.Contains(err.Error(), "invalid proof")
	}

	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	_ = ft.Hash(0)

	_, err := ft.Proof(20)
	t.Contains(err.Error(), "over size")

	{ // even
		pr, err := ft.Proof(18)
		t.NoError(err)
		t.Equal(24, len(pr))

		err = ProveFixedTreeProof(pr, nil)
		t.NoError(err)
	}

	{ // odd
		pr, err := ft.Proof(19)
		t.NoError(err)
		t.Equal(22, len(pr))

		err = ProveFixedTreeProof(pr, nil)
		t.NoError(err)
	}
}

func (t *testFixedTree) TestProveWrongHashInTheMiddle() {
	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	_ = ft.Hash(0)

	pr, err := ft.Proof(18)
	t.NoError(err)
	t.Equal(24, len(pr))

	err = ProveFixedTreeProof(pr, nil)
	t.NoError(err)

	pr[13] = []byte("showme")
	err = ProveFixedTreeProof(pr, nil)
	t.Contains(err.Error(), "wrong hash in the middle found; index=16")
}

func (t *testFixedTree) TestValidate() {
	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	_ = ft.Hash(0)

	fv, err := NewFixedTree(ft.Nodes(), nil)
	t.NoError(err)
	t.NoError(fv.IsValid(nil))

	{
		nodes := ft.Nodes()

		_, err := NewFixedTree(nodes[:4], nil)
		t.Contains(err.Error(), "invalid nodes")
	}

	{ // wrong hash
		nodes := ft.Nodes()
		nodes[13] = []byte("showme")

		fv, err := NewFixedTree(nodes, nil)
		t.NoError(err)
		err = fv.IsValid(nil)
		t.Contains(err.Error(), "wrong hash; index=4")
	}
}

func (t *testFixedTree) TestTraverse() {
	ft := NewFixedTreeGenerator(19, nil)

	keys := make([][]byte, 19)
	for i := 0; i < ft.size; i++ {
		key := []byte(fmt.Sprintf("%d", i))
		keys[i] = key
		t.NoError(ft.Add(i, key, key))
	}

	tr, err := ft.Tree()
	t.NoError(err)

	t.NoError(tr.Traverse(func(i int, key, h, v []byte) (bool, error) {
		t.Equal(keys[i], key)
		t.Equal(keys[i], v)
		t.Equal(ft.Key(i), key)
		t.Equal(ft.Hash(i), h)
		t.Equal(ft.Extra(i), v)

		return true, nil
	}))
}

func (t *testFixedTree) TestEncode() {
	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	tr, err := ft.Tree()
	t.NoError(err)

	b, err := jsonenc.Marshal(tr)
	t.NoError(err)
	t.NotNil(b)

	var uft FixedTree
	t.NoError(uft.UnmarshalJSON(b))

	t.Equal(ft.Len(), uft.Len())
	t.True(t.compareBytes(ft.Nodes(), uft.Nodes()))
}

func (t *testFixedTree) compareBytes(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if len(a[i]) != len(b[i]) {
			return false
		} else if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}

	return true
}

func (t *testFixedTree) TestDump() {
	ft := NewFixedTreeGenerator(20, nil)

	for i := 0; i < ft.size; i++ {
		t.NoError(ft.Add(i, []byte(fmt.Sprintf("%d", i)), nil))
	}

	tr, err := ft.Tree()
	t.NoError(err)
	t.NoError(tr.IsValid(nil))

	var buf bytes.Buffer
	t.NoError(tr.Dump(&buf))

	utr, err := LoadFixedTreeFromReader(bytes.NewReader(buf.Bytes()))
	t.NoError(err)
	t.NoError(utr.IsValid(nil))

	t.Equal(tr.Len(), utr.Len())
	t.Equal(tr.Root(), utr.Root())
}

func TestFixedTree(t *testing.T) {
	suite.Run(t, new(testFixedTree))
}
