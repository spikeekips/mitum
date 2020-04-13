package isaac

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/operation"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
)

type testGenesisBlockV0 struct {
	baseTestStateHandler
	localstate *Localstate
}

func (t *testGenesisBlockV0) SetupTest() {
	t.baseTestStateHandler.SetupTest()
	baseLocalstate := t.baseTestStateHandler.localstate

	localstate, err := NewLocalstate(
		leveldbstorage.NewMemStorage(baseLocalstate.Storage().Encoders(), baseLocalstate.Storage().Encoder()),
		baseLocalstate.Node(),
		TestNetworkID,
	)
	t.NoError(err)
	t.localstate = localstate
}

func (t *testGenesisBlockV0) TestNewGenesisBlock() {
	op, err := NewKVOperation(
		t.localstate.Node().Privatekey(),
		[]byte("this-is-token"),
		"showme",
		[]byte("findme"),
		nil,
	)
	t.NoError(err)

	gg, err := NewGenesisBlockV0Generator(t.localstate, []operation.Operation{op})
	t.NoError(err)

	blk, err := gg.Generate()
	t.NoError(err)

	t.Equal(base.Height(0), blk.Height())
	t.Equal(base.Round(0), blk.Round())

	pr, err := t.localstate.Storage().Seal(blk.Proposal())
	t.NoError(err)
	t.NotNil(pr)

	st, found, err := t.localstate.Storage().State(op.Key)
	t.NoError(err)
	t.True(found)

	t.Equal(st.Key(), op.Key)
}

func TestGenesisBlockV0(t *testing.T) {
	suite.Run(t, new(testGenesisBlockV0))
}
