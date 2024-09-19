package main

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/fixedtree"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestManager struct {
	isaac.BaseTestBallots
	enc encoder.Encoder
}

func (t *baseTestManager) SetupSuite() {
	t.enc = jsonenc.NewEncoder()
}

func (t *baseTestManager) workers(n int) []string {
	w := make([]string, n)

	for i := 0; i < n; i++ {
		w[i] = fmt.Sprintf("w%d", i)
	}

	return w
}

func (t *baseTestManager) ophs(n int) [][2]util.Hash {
	ophs := make([][2]util.Hash, n)

	for i := 0; i < n; i++ {
		ophs[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
	}

	return ophs
}

func (t *baseTestManager) stateKeys(n int) []string {
	sts := make([]string, n)

	for i := 0; i < n; i++ {
		sts[i] = util.ULID().String()
	}

	return sts
}

func (t *baseTestManager) stateKeysUnordered(n int) []string {
	sts := make([]string, n)

	for i := 0; i < n; i++ {
		sts[i] = util.UUID().String()
	}

	return sts
}

func (t *baseTestManager) newCreateWriter() (
	blockFileWriterCreateWriterFunc,
	func() ([]byte, error),
) {
	w := bytes.NewBuffer(nil)

	return func(base.BlockItemType, string) (string, ioWriteFunc, blockFileDoneFileFunc, error) {
			return "", w.Write, func(err error, _ string) (string, error) { return util.UUID().String(), err }, nil
		},
		func() ([]byte, error) {
			defer w.Reset()

			return w.Bytes(), nil
		}
}

func (t *baseTestManager) newProposalWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterProposalFunc {
	return newFuncBlockFileProposalWriter(t.enc, createWriter)
}

func (t *baseTestManager) newVoteproofsWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterVoteproofsFunc {
	return newFuncBlockFileVoteproofsWriter(t.enc, createWriter)
}

func (t *baseTestManager) newOperationsWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterOperationsFunc {
	return newFuncBlockFileOperationsWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newStatesWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterStatesFunc {
	return newFuncBlockFileStatesWriter(t.enc, createWriter, nil, nil)
}

func (t *baseTestManager) newMapWriter(createWriter blockFileWriterCreateWriterFunc) blockFileWriterMapFunc {
	return newFuncBlockFileMapWriter(t.enc, createWriter)
}

func (t *baseTestManager) newProposal() base.ProposalSignFact {
	fact := t.NewProposalFact(base.RawPoint(33, 44), t.Local, [][2]util.Hash{{valuehash.RandomSHA256(), valuehash.RandomSHA256()}, {valuehash.RandomSHA256(), valuehash.RandomSHA256()}})

	return t.NewProposal(t.Local, fact)
}

func (t *baseTestManager) newINITVoteproof() base.INITVoteproof {
	fact := t.NewINITBallotFact(base.RawPoint(33, 0), valuehash.RandomSHA256(), valuehash.RandomSHA256())
	vp, err := t.NewINITVoteproof(fact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	return vp
}

func (t *baseTestManager) newACCEPTVoteproof() base.ACCEPTVoteproof {
	fact := t.NewACCEPTBallotFact(base.RawPoint(33, 0), valuehash.RandomSHA256(), valuehash.RandomSHA256())

	vp, err := t.NewACCEPTVoteproof(fact, t.Local, []base.LocalNode{t.Local})
	t.NoError(err)

	return vp
}

func (t *baseTestManager) newOperationsTree() fixedtree.Tree {
	tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, 33)
	if err != nil {
		panic(err)
	}

	for i := 0; i < tw.Len(); i++ {
		t.NoError(tw.Add(uint64(i), base.NewInStateOperationFixedtreeNode(valuehash.RandomSHA256(), "")))
	}

	tr, err := tw.Tree()
	t.NoError(err)

	return tr
}

func (t *baseTestManager) newStatesTree() fixedtree.Tree {
	tw, err := fixedtree.NewWriter(base.OperationFixedtreeHint, 33)
	if err != nil {
		panic(err)
	}

	for i := 0; i < tw.Len(); i++ {
		t.NoError(tw.Add(uint64(i), fixedtree.NewBaseNode(util.ULID().String())))
	}

	tr, err := tw.Tree()
	t.NoError(err)

	return tr
}

type testCheckEnoughFailedWorkersFunc struct {
	suite.Suite
}

func (t *testCheckEnoughFailedWorkersFunc) TestNew() {
	t.Run("1 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
	})

	t.Run("0 min enough + 0 max failed", func() {
		_, _, err := newCheckEnoughFailedWorkers(0, 0)
		t.ErrorContains(err, "zero min enough")
	})
}

func (t *testCheckEnoughFailedWorkersFunc) TestEnough() {
	t.Run("enough", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.NoError(err)
		t.True(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("same worker", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w0", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.False(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})

	t.Run("error, but after closed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return nil })
		t.True(ok)
		t.True(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.False(ok)
		t.True(enough)
		t.False(failed)
	})
}

func (t *testCheckEnoughFailedWorkersFunc) TestFailed() {
	t.Run("failed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("failed, over zero", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 1)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("error, but under max failed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 2)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.False(failed)

		ok, enough, failed, err = w("w1", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)
	})

	t.Run("ok, but after closed", func() {
		w, c, err := newCheckEnoughFailedWorkers(1, 0)
		t.NoError(err)
		defer c()

		ok, enough, failed, err := w("w0", func() error { return errors.Errorf("showme") })
		t.True(ok)
		t.False(enough)
		t.True(failed)

		ok, enough, failed, err = w("w1", func() error { return nil })
		t.False(ok)
		t.False(enough)
		t.True(failed)
	})
}

func TestNewCheckEnoughFailedWorkersFunc(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testCheckEnoughFailedWorkersFunc))
}

/*
type testManager struct {
	baseTestManager
	baseTestBlock
}

func (t *testManager) newArgs() *managerArgs {
	args := newManagerArgs()

	args.NewCheckWorkersFunc = func(minEnough, maxFailed uint64) (
		checkEnoughFailedWorkerFunc,
		func(),
		error,
	) {
		return newCheckEnoughFailedWorkers(minEnough, maxFailed)
	}
	args.NewWorkersOpsFunc = serialNoShareWorkerOps
	args.StartWorker = func(context.Context, string) error { return nil }
	args.NewStatesTreeWriterFuncs = newFuncStatesTreeWriter(1<<9, 1<<9)

	_ = args.SetLogging(logging.TestNilLogging)

	return args
}

func (t *testManager) TestError() {
	t.Run("empty workers", func() {
		f := newFuncManager(t.newArgs())

		_, _, _, _, err := f(
			context.Background(),
			nil,
			3, 3,
			nil, nil,
		)
		t.ErrorContains(err, "empty workers")
	})

	t.Run("zero share", func() {
		f := newFuncManager(t.newArgs())

		_, _, _, _, err := f(context.Background(), t.workers(3),
			0, 3,
			nil, nil,
		)
		t.ErrorContains(err, "zero share")

		_, _, _, _, err = f(context.Background(), t.workers(3),
			3, 0,
			nil, nil,
		)
		t.ErrorContains(err, "zero share")
	})
}

func (t *testManager) TestDistributeOperations() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	f := newFuncManager(t.newArgs())

	workers := t.workers(3)
	pr := t.newProposal()
	ivp := t.newINITVoteproof()

	distribute, workerUnmerged, newManifest, _, err := f(
		ctx,
		workers,
		1, 1,
		ivp,
		pr,
	)
	t.NoError(err)

	workersDistributed := util.NewSingleLockedMap[string, []uint64]()

	t.NoError(distribute(func(ctx context.Context, worker string, f func(managerWorkerOperationFunc) error) error {
		var add util.CompactAppendSliceAddFunc[uint64]
		var done util.CompactAppendSliceDoneFunc[uint64]

		switch a, d, err := util.CompactAppendSliceV1[uint64](1<<9, 1<<9); {
		case err != nil:
			return err
		default:
			add = a
			done = d
		}

		if err := f(func(opindex uint64, _, _ util.Hash) error {
			add(opindex)

			return nil
		}); err != nil {
			return err
		}

		return done(func(s []uint64) error {
			_, _, err := workersDistributed.Set(worker, func(_ []uint64, found bool) ([]uint64, error) {
				if found {
					return nil, errors.Errorf("workers=%q already distributed", worker)
				}

				return slices.Clone(s), nil
			})

			return err
		})
	}))

	util.TraverseSliceV1(workers, func(_ int, worker string) (bool, error) {
		i, found := workersDistributed.Value(worker)
		t.True(found)
		t.NotEmpty(i)

		return true, nil
	})

	_ = workerUnmerged
	_ = newManifest
}

// FIXME empty ophs

func TestManager(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testManager))
}
*/
