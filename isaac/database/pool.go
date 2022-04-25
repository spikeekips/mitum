package database

import (
	"bytes"
	"context"
	"encoding/json"
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
	st             *leveldbstorage.RWStorage
	lastvoteproofs *util.Locked
}

// BLOCK will be used for ProposalPool
// BLOCK clean old proposals

func NewTempPool(f string, encs *encoder.Encoders, enc encoder.Encoder) (*TempPool, error) {
	st, err := leveldbstorage.NewRWStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempPoolDatabase")
	}

	return newTempPool(st, encs, enc)
}

func newTempPool(st *leveldbstorage.RWStorage, encs *encoder.Encoders, enc encoder.Encoder) (*TempPool, error) {
	db := &TempPool{
		baseLeveldb:    newBaseLeveldb(st, encs, enc),
		st:             st,
		lastvoteproofs: util.EmptyLocked(),
	}

	if err := db.loadLastVoteproofs(); err != nil {
		return nil, errors.Wrap(err, "failed newTempPool")
	}

	return db, nil
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

	key, orderedkey := newNewOperationLeveldbKeys(facthash)
	switch found, err := db.st.Exists(key); {
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
	batch.Put(key, b)
	batch.Put(orderedkey, facthash.Bytes())

	batch.Put(leveldbNewOperationKeysKey(facthash), joinNewOperationLeveldbOrderedKeys(key, orderedkey))

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

func (db *TempPool) LastVoteproofs() (base.INITVoteproof, base.ACCEPTVoteproof, bool, error) {
	switch i, _ := db.lastvoteproofs.Value(); {
	case i == nil:
		return nil, nil, false, nil
	default:
		j, ok := i.([2]base.Voteproof)
		if !ok {
			return nil, nil, false, errors.Errorf("invalid last voteproofs")
		}

		return j[0].(base.INITVoteproof), j[1].(base.ACCEPTVoteproof), true, nil
	}
}

func (db *TempPool) SetLastVoteproofs(ivp base.INITVoteproof, avp base.ACCEPTVoteproof) error {
	e := util.StringErrorFunc("failed to set last voteproofs")

	if !ivp.Point().Point.Equal(avp.Point().Point) {
		return e(nil, "voteproofs should have same point")
	}

	if _, err := db.lastvoteproofs.Set(func(interface{}) (interface{}, error) {
		vps := [2]base.Voteproof{ivp, avp}
		b, err := db.marshal(vps)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if err := db.st.Put(leveldbKeyLastVoteproofs, b, nil); err != nil {
			return nil, errors.Wrap(err, "")
		}

		return vps, nil
	}); err != nil {
		return e(err, "failed to set last voteproofs")
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
			infokey := leveldbNewOperationKeysKey(h)
			b, found, err := db.st.Get(infokey)
			switch {
			case err != nil:
				return errors.Wrap(err, "")
			case !found:
				return nil
			}

			key, orderedkey, err := db.loadNewOperationKeys(b)
			if err != nil {
				return errors.Wrap(err, "")
			}

			removekeysch <- infokey
			removekeysch <- key
			removekeysch <- orderedkey

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

func (db *TempPool) loadNewOperationKeys(b []byte) (key []byte, orderedkey []byte, err error) {
	if b == nil {
		return nil, nil, nil
	}

	i := bytes.LastIndex(b, leveldbNewOperationOrderedKeysJoinedSep)
	if i < 0 {
		return nil, nil, errors.Errorf("unknown NewOperations info key format")
	}

	return b[:i], b[i+len(leveldbNewOperationOrderedKeysJoinSep):], nil
}

func (db *TempPool) loadLastVoteproofs() error {
	e := util.StringErrorFunc("failed to load last voteproofs")

	b, found, err := db.st.Get(leveldbKeyLastVoteproofs)
	switch {
	case err != nil:
		return e(err, "")
	case !found:
		return nil
	}

	enc, raw, err := db.readEncoder(b)
	if err != nil {
		return e(err, "")
	}

	var u [2]json.RawMessage
	if err := enc.Unmarshal(raw, &u); err != nil {
		return e(err, "")
	}

	var ivp base.INITVoteproof
	switch hinter, err := enc.Decode(u[0]); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty init voteproof")
	default:
		i, ok := hinter.(base.INITVoteproof)
		if !ok {
			return e(nil, "expected INITVoteproof, but %T", hinter)
		}

		ivp = i
	}

	var avp base.ACCEPTVoteproof
	switch hinter, err := enc.Decode(u[1]); {
	case err != nil:
		return e(err, "")
	case hinter == nil:
		return e(nil, "empty accept voteproof")
	default:
		i, ok := hinter.(base.ACCEPTVoteproof)
		if !ok {
			return e(nil, "expected ACCEPTVoteproof, but %T", hinter)
		}

		avp = i
	}

	_ = db.lastvoteproofs.SetValue([2]base.Voteproof{ivp, avp})

	return nil
}

func newNewOperationLeveldbKeys(facthash util.Hash) (key []byte, orderedkey []byte) {
	return leveldbNewOperationKey(facthash), leveldbNewOperationOrderedKey(facthash)
}

func joinNewOperationLeveldbOrderedKeys(a, b []byte) []byte {
	return bytes.Join([][]byte{a, b}, leveldbNewOperationOrderedKeysJoinSep)
}
