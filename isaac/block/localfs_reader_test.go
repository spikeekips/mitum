package isaacblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testBaseLocalBlockFS struct {
	isaac.BaseTestBallots
	isaacdatabase.BaseTestDatabase
}

func (t *testBaseLocalBlockFS) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: BlockMapHint, Instance: BlockMap{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITVoteproofHint, Instance: isaac.INITVoteproof{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTVoteproofHint, Instance: isaac.ACCEPTVoteproof{}}))

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.OperationFixedtreeHint, Instance: base.OperationFixedtreeNode{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.StateFixedtreeHint, Instance: fixedtree.BaseNode{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotFactHint, Instance: isaac.INITBallotFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotFactHint, Instance: isaac.ACCEPTBallotFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignedFactHint, Instance: isaac.INITBallotSignedFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotSignedFactHint, Instance: isaac.ACCEPTBallotSignedFact{}}))
}

func (t *testBaseLocalBlockFS) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testBaseLocalBlockFS) voteproofs(point base.Point) (base.INITVoteproof, base.ACCEPTVoteproof) {
	_, nodes := isaac.NewTestSuffrage(1, t.Local)

	ifact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	afact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, nodes)
	t.NoError(err)

	return ivp, avp
}

func (t *testBaseLocalBlockFS) walkDirectory(root string, a ...any) {
	if len(a) > 0 {
		t.T().Logf(a[0].(string), a[1:]...)
	}

	filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.T().Logf("error: %+v", err)

			return err
		}

		if info.IsDir() {
			t.T().Log("directory:", path)

			return nil
		}

		t.T().Log("file:", path)

		f, err := os.Open(path)
		t.NoError(err)
		var b []byte
		if strings.HasSuffix(path, ".gz") {
			gr, err := gzip.NewReader(f)
			t.NoError(err)
			i, err := io.ReadAll(gr)
			t.NoError(err)

			b = i
		} else {
			i, err := io.ReadAll(f)
			t.NoError(err)

			b = i
		}
		t.T().Log("   >", string(b))

		return nil
	})
}

func (t *testBaseLocalBlockFS) preparefs(point base.Point) (
	*LocalFSWriter,
	base.ProposalSignedFact,
	[]base.Operation,
	fixedtree.Tree,
	[]base.State,
	fixedtree.Tree,
	[]base.Voteproof,
) {
	ctx := context.Background()

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Local, t.NodePolicy.NetworkID())
	t.NoError(err)

	// NOTE set manifest
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	t.NoError(fs.SetManifest(ctx, manifest))

	// NOTE set proposal
	pr := isaac.NewProposalSignedFact(isaac.NewProposalFact(point, t.Local.Address(), []util.Hash{valuehash.RandomSHA256()}))
	_ = pr.Sign(t.Local.Privatekey(), t.NodePolicy.NetworkID())
	t.NoError(fs.SetProposal(ctx, pr))

	// NOTE set operations
	ops := make([]base.Operation, 3)
	opstreeg, err := fixedtree.NewWriter(base.OperationFixedtreeHint, uint64(len(ops)))
	t.NoError(err)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.NodePolicy.NetworkID())
		ops[i] = op

		node := base.NewInStateOperationFixedtreeNode(op.Fact().Hash(), "")

		t.NoError(fs.SetOperation(context.Background(), uint64(i), op))
		t.NoError(opstreeg.Add(uint64(i), node))
	}

	t.NoError(fs.SetOperationsTree(context.Background(), opstreeg))

	opstree, err := opstreeg.Tree()
	t.NoError(err)

	// NOTE set states
	stts := make([]base.State, 3)
	sttstreeg, err := fixedtree.NewWriter(base.StateFixedtreeHint, uint64(len(stts)))
	t.NoError(err)
	for i := range stts {
		key := fmt.Sprintf("state-key-%d-%s", i, util.UUID().String())
		stts[i] = base.NewBaseState(
			point.Height(),
			key,
			base.NewDummyStateValue(util.UUID().String()),
			valuehash.RandomSHA256(),
			nil,
		)
		node := fixedtree.NewBaseNode(key)
		t.NoError(sttstreeg.Add(uint64(i), node))

		t.NoError(fs.SetState(context.Background(), uint64(i), stts[i]))
	}

	_, err = fs.SetStatesTree(context.Background(), sttstreeg)
	t.NoError(err)

	sttstree, err := sttstreeg.Tree()
	t.NoError(err)

	// NOTE set voteproofs
	ivp, avp := t.voteproofs(point)
	t.NoError(fs.SetINITVoteproof(ctx, ivp))
	t.NoError(fs.SetACCEPTVoteproof(ctx, avp))

	return fs, pr, ops, opstree, stts, sttstree, []base.Voteproof{ivp, avp}
}

type testLocalFSReader struct {
	testBaseLocalBlockFS
}

func (t *testLocalFSReader) TestNew() {
	ctx := context.Background()

	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.preparefs(point)
	_, err := fs.Save(ctx)
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)
	t.NotNil(r)

	_ = (interface{})(r).(isaac.BlockReader)

	t.Equal(filepath.Join(fs.root, fs.heightbase), r.root)
}

func (t *testLocalFSReader) TestMap() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.preparefs(point)
	_, err := fs.Save(context.Background())
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	m, found, err := r.Map()
	t.NoError(err)
	t.True(found)

	t.NoError(m.IsValid(t.NodePolicy.NetworkID()))
}

func (t *testLocalFSReader) TestReader() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.preparefs(point)
	m, err := fs.Save(context.Background())
	t.NoError(err)

	{
		t.walkDirectory(fs.root)

		b, _ := util.MarshalJSONIndent(m)
		t.T().Log("blockmap:", string(b))
	}

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.Reader(base.BlockMapItemType("findme"))
		t.Error(err)
		t.False(found)
		t.Nil(f)

		t.ErrorContains(err, "unknown block map item type")
	})

	t.Run("all knowns", func() {
		types := []base.BlockMapItemType{
			base.BlockMapItemTypeProposal,
			base.BlockMapItemTypeOperations,
			base.BlockMapItemTypeOperationsTree,
			base.BlockMapItemTypeStates,
			base.BlockMapItemTypeStatesTree,
			base.BlockMapItemTypeVoteproofs,
		}

		for i := range types {
			f, found, err := r.Reader(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(f, "type: %q", types[i])
		}
	})

	t.Run("known and found", func() {
		f, found, err := r.Reader(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)
		defer f.Close()

		b, err := io.ReadAll(f)
		t.NoError(err)
		hinter, err := t.Enc.Decode(b)
		t.NoError(err)

		_ = hinter.(base.ProposalSignedFact)
	})

	t.Run("known, but not found", func() {
		// NOTE remove
		fname, _ := BlockFileName(base.BlockMapItemTypeOperations, t.Enc.Hint().Type().String())
		t.NoError(os.Remove(filepath.Join(r.root, fname)))

		f, found, err := r.Reader(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})
}

func (t *testLocalFSReader) TestChecksumReader() {
	point := base.RawPoint(33, 44)
	fs, _, _, _, _, _, _ := t.preparefs(point)
	m, err := fs.Save(context.Background())
	t.NoError(err)

	{
		t.walkDirectory(fs.root)

		b, _ := util.MarshalJSONIndent(m)
		t.T().Log("blockmap:", string(b))
	}

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.ChecksumReader(base.BlockMapItemType("findme"))
		t.Error(err)
		t.False(found)
		t.Nil(f)

		t.ErrorContains(err, "unknown block map item type")
	})

	t.Run("all knowns", func() {
		types := []base.BlockMapItemType{
			base.BlockMapItemTypeProposal,
			base.BlockMapItemTypeOperations,
			base.BlockMapItemTypeOperationsTree,
			base.BlockMapItemTypeStates,
			base.BlockMapItemTypeStatesTree,
			base.BlockMapItemTypeVoteproofs,
		}

		for i := range types {
			f, found, err := r.ChecksumReader(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(f, "type: %q", types[i])
		}
	})

	t.Run("known and found", func() {
		f, found, err := r.ChecksumReader(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)
		defer f.Close()

		b, err := io.ReadAll(f)
		t.NoError(err)
		hinter, err := t.Enc.Decode(b)
		t.NoError(err)

		_ = hinter.(base.ProposalSignedFact)
	})

	t.Run("known, but not found", func() {
		// NOTE remove
		fname, _ := BlockFileName(base.BlockMapItemTypeOperations, t.Enc.Hint().Type().String())
		t.NoError(os.Remove(filepath.Join(r.root, fname)))

		f, found, err := r.ChecksumReader(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})
}

func (t *testLocalFSReader) TestItem() {
	point := base.RawPoint(33, 44)
	fs, pr, ops, opstree, stts, sttstree, vps := t.preparefs(point)

	_, err := fs.Save(context.Background())
	t.NoError(err)

	r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
	t.NoError(err)

	t.Run("unknown", func() {
		f, found, err := r.Item(base.BlockMapItemType("findme"))
		t.NoError(err)
		t.False(found)
		t.Nil(f)
	})

	t.Run("all knowns", func() {
		types := []base.BlockMapItemType{
			base.BlockMapItemTypeProposal,
			base.BlockMapItemTypeOperations,
			base.BlockMapItemTypeOperationsTree,
			base.BlockMapItemTypeStates,
			base.BlockMapItemTypeStatesTree,
			base.BlockMapItemTypeVoteproofs,
		}

		for i := range types {
			v, found, err := r.Item(types[i])
			t.NoError(err, "type: %q", types[i])
			t.True(found, "type: %q", types[i])
			t.NotNil(v, "type: %q", types[i])
		}
	})

	t.Run("proposal", func() {
		v, found, err := r.Item(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		upr, ok := v.(base.ProposalSignedFact)
		t.True(ok)

		base.EqualProposalSignedFact(t.Assert(), pr, upr)
	})

	t.Run("operations", func() {
		v, found, err := r.Item(base.BlockMapItemTypeOperations)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uops, ok := v.([]base.Operation)
		t.True(ok)

		t.Equal(len(ops), len(uops))

		sort.Slice(ops, func(i, j int) bool {
			return bytes.Compare(ops[i].Fact().Hash().Bytes(), ops[j].Fact().Hash().Bytes()) > 0
		})
		sort.Slice(uops, func(i, j int) bool {
			return bytes.Compare(uops[i].Fact().Hash().Bytes(), uops[j].Fact().Hash().Bytes()) > 0
		})

		for i := range ops {
			a := ops[i]
			b := uops[i]

			base.EqualOperation(t.Assert(), a, b)
		}
	})

	t.Run("operations tree", func() {
		v, found, err := r.Item(base.BlockMapItemTypeOperationsTree)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uopstree, ok := v.(fixedtree.Tree)
		t.True(ok)

		t.NoError(opstree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			if i := uopstree.Node(index); i == nil {
				return false, util.ErrNotFound.Errorf("node not found")
			} else if !n.Equal(i) {
				return false, errors.Errorf("not equal")
			}

			return true, nil
		}))
	})

	t.Run("states", func() {
		v, found, err := r.Item(base.BlockMapItemTypeStates)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		ustts, ok := v.([]base.State)
		t.True(ok)

		t.Equal(len(stts), len(ustts))

		sort.Slice(stts, func(i, j int) bool {
			return strings.Compare(stts[i].Key(), stts[j].Key()) > 0
		})
		sort.Slice(ustts, func(i, j int) bool {
			return strings.Compare(ustts[i].Key(), ustts[j].Key()) > 0
		})

		for i := range stts {
			a := stts[i]
			b := ustts[i]

			t.True(base.IsEqualState(a, b))
		}
	})

	t.Run("states tree", func() {
		v, found, err := r.Item(base.BlockMapItemTypeStatesTree)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		usttstree, ok := v.(fixedtree.Tree)
		t.True(ok)

		t.NoError(sttstree.Traverse(func(index uint64, n fixedtree.Node) (bool, error) {
			if i := usttstree.Node(index); i == nil {
				return false, util.ErrNotFound.Errorf("node not found")
			} else if !n.Equal(i) {
				return false, errors.Errorf("not equal")
			}

			return true, nil
		}))
	})

	t.Run("voteproofs", func() {
		v, found, err := r.Item(base.BlockMapItemTypeVoteproofs)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		uvps, ok := v.([]base.Voteproof)
		t.True(ok)
		t.Equal(2, len(uvps))

		base.EqualVoteproof(t.Assert(), vps[0], uvps[0])
		base.EqualVoteproof(t.Assert(), vps[1], uvps[1])
	})
}

func (t *testLocalFSReader) TestWrongChecksum() {
	point := base.RawPoint(33, 44)
	fs, pr, _, _, _, _, _ := t.preparefs(point)

	_, err := fs.Save(context.Background())
	t.NoError(err)

	t.walkDirectory(t.Root)

	var root string
	t.Run("load proposal before modifying", func() {
		r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
		t.NoError(err)

		root = r.root

		v, found, err := r.Item(base.BlockMapItemTypeProposal)
		t.NoError(err)
		t.True(found)
		t.NotNil(v)

		upr, ok := v.(base.ProposalSignedFact)
		t.True(ok)

		base.EqualProposalSignedFact(t.Assert(), pr, upr)
	})

	// NOTE modify proposal.json
	i, _ := BlockFileName(base.BlockMapItemTypeProposal, t.Enc.Hint().Type().String())
	path := filepath.Join(root, i)
	f, err := os.Open(path)
	t.NoError(err)

	b, err := io.ReadAll(f)
	t.NoError(err)

	b = append(b, '\n')
	nf, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	t.NoError(err)
	_, err = nf.Write(b)
	t.NoError(err)
	nf.Close()

	t.Run("load proposal after modifying", func() {
		r, err := NewLocalFSReaderFromHeight(t.Root, point.Height(), t.Enc)
		t.NoError(err)

		v, found, err := r.Item(base.BlockMapItemTypeProposal)
		t.Error(err)
		t.ErrorContains(err, "checksum mismatch")
		t.True(found)
		t.NotNil(v)
	})
}

func TestLocalFSReader(t *testing.T) {
	defer goleak.VerifyNone(t,
		goleak.IgnoreTopFunction("github.com/syndtr/goleveldb/leveldb.(*DB).mpoolDrain"),
	)

	suite.Run(t, new(testLocalFSReader))
}
