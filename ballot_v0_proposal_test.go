package mitum

import (
	"testing"

	"github.com/stretchr/testify/suite"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/hint"
	"github.com/spikeekips/mitum/key"
	"github.com/spikeekips/mitum/valuehash"
)

type testBallotV0Proposal struct {
	suite.Suite

	pk key.BTCPrivatekey
}

func (t *testBallotV0Proposal) SetupSuite() {
	_ = hint.RegisterType(key.BTCPrivatekey{}.Hint().Type(), "btc-privatekey")
	_ = hint.RegisterType(key.BTCPublickey{}.Hint().Type(), "btc-publickey")
	_ = hint.RegisterType(ProposalBallotType, "proposal")

	t.pk, _ = key.NewBTCPrivatekey()
}

func (t *testBallotV0Proposal) TestNew() {
	ib := ProposalV0{
		BaseBallotV0: BaseBallotV0{
			height: Height(10),
			round:  Round(0),
			node:   NewShortAddress("test-for-proposal"),
		},
	}

	t.NotEmpty(ib)

	_ = (interface{})(ib).(Ballot)
	t.Implements((*Ballot)(nil), ib)
}

func (t *testBallotV0Proposal) TestGenerateHash() {
	ib := ProposalV0{
		BaseBallotV0: BaseBallotV0{
			height: Height(10),
			round:  Round(0),
			node:   NewShortAddress("test-for-proposal"),
		},
		seals: []valuehash.Hash{
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		},
	}

	h, err := ib.GenerateBodyHash(nil)
	t.NoError(err)
	t.NotNil(h)
	t.NotEmpty(h)

	bh, err := ib.GenerateBodyHash(nil)
	t.NoError(err)
	t.NotNil(bh)
	t.NotEmpty(bh)
}

func (t *testBallotV0Proposal) TestSign() {
	ib := ProposalV0{
		BaseBallotV0: BaseBallotV0{
			height: Height(10),
			round:  Round(0),
			node:   NewShortAddress("test-for-proposal"),
		},
		seals: []valuehash.Hash{
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
			valuehash.RandomSHA256(),
		},
	}

	t.Nil(ib.Hash())
	t.Nil(ib.BodyHash())
	t.Nil(ib.Signer())
	t.Nil(ib.Signature())
	t.True(ib.SignedAt().IsZero())

	t.NoError(ib.Sign(t.pk, nil))

	t.NotNil(ib.Hash())
	t.NotNil(ib.BodyHash())
	t.NotNil(ib.Signer())
	t.NotNil(ib.Signature())
	t.False(ib.SignedAt().IsZero())

	t.NoError(ib.Signer().Verify(ib.BodyHash().Bytes(), ib.Signature()))

	// invalid signature
	unknownPK, _ := key.NewBTCPrivatekey()
	err := unknownPK.Publickey().Verify(ib.BodyHash().Bytes(), ib.Signature())
	t.True(xerrors.Is(err, key.SignatureVerificationFailedError))
}

func TestBallotV0Proposal(t *testing.T) {
	suite.Run(t, new(testBallotV0Proposal))
}