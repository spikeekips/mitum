package database

import (
	"bytes"
	"context"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempPool struct {
	*baseLeveldb
	st *leveldbstorage.RWStorage
}

// BLOCK will be used for ProposalPool
// BLOCK clean old proposals

func NewTempPool(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempPool, error) {
	st, err := leveldbstorage.NewRWStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempPoolDatabase")
	}

	return newTempPool(st, encs, enc), nil
}

func newTempPool(st *leveldbstorage.RWStorage, encs *encoder.Encoders, enc encoder.Encoder) *TempPool {
	return &TempPool{
		baseLeveldb: newBaseLeveldb(st, encs, enc),
		st:          st,
	}
}

func (db *TempPool) Proposal(h util.Hash) (base.ProposalSignedFact, bool, error) {
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

func (db *TempPool) ProposalByPoint(
	point base.Point,
	proposer base.Address,
) (base.ProposalSignedFact, bool, error) {
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

func (db *TempPool) SetProposal(pr base.ProposalSignedFact) (bool, error) {
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

func (db *TempPool) NewOperation(_ context.Context, facthash util.Hash) (base.Operation, bool, error) {
	e := util.StringErrorFunc("failed to find operation")

	switch b, found, err := db.st.Get(leveldbNewOperationKey(facthash)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		op, err := db.loadOperation(b)
		if err != nil {
			return nil, false, e(err, "")
		}
		return op, true, nil
	}
}

func (db *TempPool) NewOperationHashes(
	ctx context.Context,
	limit uint64,
	filter func(facthash util.Hash) (bool, error),
) ([]util.Hash, error) {
	e := util.StringErrorFunc("failed to find new operations")

	ops := make([]util.Hash, limit)
	var removes []util.Hash

	if filter == nil {
		filter = func(facthash util.Hash) (bool, error) { return true, nil }
	}

	var i uint64
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered),
		func(_ []byte, b []byte) (bool, error) {
			h := valuehash.Bytes(b)

			switch ok, err := filter(h); {
			case err != nil:
				return false, errors.Wrap(err, "")
			case !ok:
				removes = append(removes, h)

				return true, nil
			}

			ops[i] = h
			i++

			if i == limit {
				return false, nil
			}

			return true, nil
		},
		true,
	); err != nil {
		return nil, e(err, "")
	}

	if i < limit {
		ops = ops[:i]
	}

	if len(removes) > 0 {
		if err := db.RemoveNewOperations(ctx, removes); err != nil {
			return nil, e(err, "")
		}
	}

	return ops, nil
}

func (db *TempPool) SetNewOperation(_ context.Context, op base.Operation) (bool, error) {
	e := util.StringErrorFunc("failed to put operation")

	facthash := op.Fact().Hash()

	info := newNewOperationLeveldbInfo(facthash)
	switch found, err := db.st.Exists(info.Key); {
	case err != nil:
		return false, e(err, "")
	case found:
		return false, nil
	}

	b, err := db.marshal(op)
	if err != nil {
		return false, e(err, "failed to marshal operation")
	}

	batch := new(leveldb.Batch)
	batch.Put(info.Key, b)
	batch.Put(info.OrderedKey, facthash.Bytes())

	b, err = db.marshal(info)
	if err != nil {
		return false, e(err, "failed to marshal newOperationLeveldbInfo")
	}

	batch.Put(leveldbNewOperationInfoKey(facthash), b)

	if err := db.st.Write(batch, nil); err != nil {
		return false, e(err, "")
	}

	return true, nil
}

func (db *TempPool) RemoveNewOperations(ctx context.Context, facthashes []util.Hash) error {
	e := util.StringErrorFunc("failed to remove NewOperations")

	hs := make([]util.Hash, 3333)
	for i := range facthashes {
		hs[i%len(hs)] = facthashes[i]

		if i == len(hs)-1 {
			if err := db.removeNewOperations(ctx, hs); err != nil {
				return e(err, "")
			}

			hs = nil
		}
	}

	if len(facthashes)%len(hs) > 0 {
		if err := db.removeNewOperations(ctx, hs); err != nil {
			return e(err, "")
		}
	}

	return nil
}

func (db *TempPool) removeNewOperations(ctx context.Context, facthashes []util.Hash) error {
	worker := util.NewErrgroupWorker(ctx, math.MaxInt32)
	defer worker.Close()

	batch := new(leveldb.Batch)

	removekeysch := make(chan []byte)
	donech := make(chan struct{})
	go func() {
		for i := range removekeysch {
			batch.Delete(i)
		}

		donech <- struct{}{}
	}()

	for i := range facthashes {
		h := facthashes[i]
		if h == nil {
			break
		}

		if err := worker.NewJob(func(context.Context, uint64) error {
			infokey := leveldbNewOperationInfoKey(h)
			b, found, err := db.st.Get(infokey)
			switch {
			case err != nil:
				return errors.Wrap(err, "")
			case !found:
				return nil
			}

			info, err := db.loadNewOperationInfo(b)
			if err != nil {
				return errors.Wrap(err, "")
			}

			removekeysch <- infokey
			removekeysch <- info.Key
			removekeysch <- info.OrderedKey

			return nil
		}); err != nil {
			break
		}
	}
	worker.Done()

	if err := worker.Wait(); err != nil {
		return errors.Wrap(err, "")
	}

	close(removekeysch)
	<-donech

	if err := db.st.Write(batch, nil); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}

func (db *TempPool) loadProposal(b []byte) (base.ProposalSignedFact, error) {
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

func (db *TempPool) loadOperation(b []byte) (base.Operation, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load operation")

	switch hinter, err := db.readHinter(b); {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, e(nil, "empty hinter")
	default:
		i, ok := hinter.(base.Operation)
		if !ok {
			return nil, e(nil, "expected Operation, but %T", hinter)
		}

		return i, nil
	}
}

func (db *TempPool) loadNewOperationInfo(b []byte) (NewOperationLeveldbInfo, error) {
	if b == nil {
		return NewOperationLeveldbInfo{}, nil
	}

	e := util.StringErrorFunc("failed to load NewOperationLeveldbInfo")

	enc, raw, err := db.readEncoder(b)
	if err != nil {
		return NewOperationLeveldbInfo{}, e(err, "")
	}

	var u NewOperationLeveldbInfo
	if err := enc.Unmarshal(raw, &u); err != nil {
		return NewOperationLeveldbInfo{}, e(err, "")
	}

	return u, nil
}

type NewOperationLeveldbInfo struct {
	Key        []byte
	OrderedKey []byte
}

func newNewOperationLeveldbInfo(facthash util.Hash) NewOperationLeveldbInfo {
	return NewOperationLeveldbInfo{
		Key:        leveldbNewOperationKey(facthash),
		OrderedKey: leveldbNewOperationOrderedKey(facthash),
	}
}
