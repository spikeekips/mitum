package isaac

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempPoolDatabase struct {
	*baseLeveldbDatabase
	st *leveldbstorage.RWStorage
}

// BLOCK used for ProposalPool
// BLOCK clean old proposals

func NewTempPoolDatabase(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempPoolDatabase, error) {
	st, err := leveldbstorage.NewRWStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempPoolDatabase")
	}

	return newTempPoolDatabase(st, encs, enc), nil
}

func newTempPoolDatabase(st *leveldbstorage.RWStorage, encs *encoder.Encoders, enc encoder.Encoder) *TempPoolDatabase {
	return &TempPoolDatabase{
		baseLeveldbDatabase: newBaseLeveldbDatabase(st, encs, enc),
		st:                  st,
	}
}

func (db *TempPoolDatabase) Proposal(h util.Hash) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to find proposal by hash")

	switch b, found, err := db.st.Get(leveldbProposalKey(h)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		pr, err := db.loadProposal(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return pr, true, nil
	}
}

func (db *TempPoolDatabase) ProposalByPoint(point base.Point, proposer base.Address) (base.ProposalSignedFact, bool, error) {
	e := util.StringErrorFunc("failed to find proposal by point")

	switch b, found, err := db.st.Get(leveldbProposalPointKey(point, proposer)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		pr, found, err := db.Proposal(valuehash.NewBytes(b))
		if err != nil {
			return nil, false, e(err, "")
		}

		return pr, found, nil
	}
}

func (db *TempPoolDatabase) SetProposal(pr base.ProposalSignedFact) (bool, error) {
	e := util.StringErrorFunc("failed to put proposal")

	key := leveldbProposalKey(pr.Fact().Hash())
	switch found, err := db.st.Exists(key); {
	case err != nil:
		return false, e(err, "")
	case found:
		return false, nil
	}

	batch := new(leveldb.Batch)

	// NOTE remove old proposals
	top := leveldbProposalPointKey(pr.ProposalFact().Point().Decrease(), nil)
	if err := db.st.Iter(leveldbutil.BytesPrefix(leveldbKeyPrefixProposalByPoint), func(key, b []byte) (bool, error) {
		if bytes.Compare(key[:len(top)], top) > 0 {
			return false, nil
		}

		batch.Delete(key)
		batch.Delete(leveldbProposalKey(valuehash.Bytes(b)))

		return true, nil
	}, true); err != nil {
		return false, e(err, "failed to find old proposals")
	}

	b, err := db.marshal(pr)
	if err != nil {
		return false, e(err, "failed to marshal proposal")
	}

	batch.Put(leveldbProposalKey(pr.Fact().Hash()), b)
	batch.Put(
		leveldbProposalPointKey(pr.ProposalFact().Point(), pr.ProposalFact().Proposer()),
		pr.Fact().Hash().Bytes(),
	)

	if err := db.st.Write(batch, nil); err != nil {
		return false, e(err, "")
	}

	return true, nil
}

func (db *TempPoolDatabase) loadProposal(b []byte) (base.ProposalSignedFact, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load proposal")

	hinter, err := db.readHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, e(nil, "empty proposal")
	}

	switch i, ok := hinter.(base.ProposalSignedFact); {
	case !ok:
		return nil, e(nil, "not ProposalSignedFact: %T", hinter)
	default:
		return i, nil
	}
}
