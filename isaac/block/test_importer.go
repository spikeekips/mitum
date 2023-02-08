//go:build test
// +build test

package isaacblock

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
)

type DummyBlockImporter struct {
	WriteMapf     func(base.BlockMap) error
	WriteItemf    func(base.BlockMapItemType, io.Reader) error
	Savef         func(context.Context) (func(context.Context) error, error)
	Mergef        func(context.Context) error
	CancelImportf func(context.Context) error
}

func (im *DummyBlockImporter) Reader() (isaac.BlockReader, error) {
	return nil, nil
}

func (im *DummyBlockImporter) WriteMap(m base.BlockMap) error {
	if im.WriteMapf != nil {
		return im.WriteMapf(m)
	}

	return nil
}

func (im *DummyBlockImporter) WriteItem(item base.BlockMapItemType, r io.Reader) error {
	if im.WriteItemf != nil {
		return im.WriteItemf(item, r)
	}

	return nil
}

func (im *DummyBlockImporter) Save(ctx context.Context) (func(context.Context) error, error) {
	if im.Savef != nil {
		return im.Savef(ctx)
	}

	return func(context.Context) error { return nil }, nil
}

func (im *DummyBlockImporter) Merge(ctx context.Context) error {
	if im.Mergef != nil {
		return im.Mergef(ctx)
	}

	return nil
}

func (im *DummyBlockImporter) CancelImport(ctx context.Context) error {
	if im.CancelImportf != nil {
		return im.CancelImportf(ctx)
	}

	return nil
}

type BaseTestLocalBlockFS struct {
	isaac.BaseTestBallots
	isaacdatabase.BaseTestDatabase
}

func (t *BaseTestLocalBlockFS) SetupSuite() {
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
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.INITBallotSignFactHint, Instance: isaac.INITBallotSignFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.ACCEPTBallotSignFactHint, Instance: isaac.ACCEPTBallotSignFact{}}))
}

func (t *BaseTestLocalBlockFS) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *BaseTestLocalBlockFS) Voteproofs(point base.Point) (base.INITVoteproof, base.ACCEPTVoteproof) {
	_, nodes := isaac.NewTestSuffrage(1, t.Local)

	ifact := t.NewINITBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	ivp, err := t.NewINITVoteproof(ifact, t.Local, nodes)
	t.NoError(err)

	afact := t.NewACCEPTBallotFact(point, valuehash.RandomSHA256(), valuehash.RandomSHA256())
	avp, err := t.NewACCEPTVoteproof(afact, t.Local, nodes)
	t.NoError(err)

	return ivp, avp
}

func (t *BaseTestLocalBlockFS) WalkFS(root string, a ...any) {
	if len(a) > 0 {
		t.T().Logf(a[0].(string), a[1:]...)
	}

	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			t.T().Logf("error: %+v", err)

			return err
		}

		if info.IsDir() {
			t.T().Log("directory:", path)

			return nil
		}

		t.T().Log("file:", path)

		return nil
	})
}

func (t *BaseTestLocalBlockFS) PrintFS(root string, a ...any) {
	if len(a) > 0 {
		t.T().Logf(a[0].(string), a[1:]...)
	}

	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
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

func (t *BaseTestLocalBlockFS) PrepareFS(point base.Point, prev util.Hash) (
	*LocalFSWriter,
	base.ProposalSignFact,
	[]base.Operation,
	fixedtree.Tree,
	[]base.State,
	fixedtree.Tree,
	[]base.Voteproof,
) {
	ctx := context.Background()

	fs, err := NewLocalFSWriter(t.Root, point.Height(), t.Enc, t.Local, t.LocalParams.NetworkID())
	t.NoError(err)

	// NOTE set manifest
	manifest := base.NewDummyManifest(point.Height(), valuehash.RandomSHA256())
	if prev != nil {
		manifest.SetPrevious(prev)
	}

	t.NoError(fs.SetManifest(ctx, manifest))

	// NOTE set proposal
	pr := isaac.NewProposalSignFact(isaac.NewProposalFact(point, t.Local.Address(), []util.Hash{valuehash.RandomSHA256()}))
	_ = pr.Sign(t.Local.Privatekey(), t.LocalParams.NetworkID())
	t.NoError(fs.SetProposal(ctx, pr))

	// NOTE set operations
	ops := make([]base.Operation, 3)
	opstreeg, err := fixedtree.NewWriter(base.OperationFixedtreeHint, uint64(len(ops)))
	t.NoError(err)
	for i := range ops {
		fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
		op, _ := isaac.NewDummyOperation(fact, t.Local.Privatekey(), t.LocalParams.NetworkID())
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
	ivp, avp := t.Voteproofs(point)
	t.NoError(fs.SetINITVoteproof(ctx, ivp))
	t.NoError(fs.SetACCEPTVoteproof(ctx, avp))

	return fs, pr, ops, opstree, stts, sttstree, []base.Voteproof{ivp, avp}
}
