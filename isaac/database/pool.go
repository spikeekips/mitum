package isaacdatabase

import (
	"bytes"
	"context"
	"encoding/json"
	"math"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempPool struct {
	*baseLeveldb
	st             *leveldbstorage.RWStorage
	lastvoteproofs *util.Locked
}

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

func (db *TempPool) Proposal(h util.Hash) (pr base.ProposalSignedFact, found bool, _ error) {
	e := util.StringErrorFunc("failed to find proposal by hash")

	switch b, found, err := db.st.Get(leveldbProposalKey(h)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &pr); err != nil {
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
	cleanpoint := pr.ProposalFact().Point().
		PrevHeight().
		PrevHeight().
		PrevHeight()

	if cleanpoint.Height() > base.NilHeight {
		top := leveldbProposalPointKey(cleanpoint, nil)

		if err := db.st.Iter(
			leveldbutil.BytesPrefix(leveldbKeyPrefixProposalByPoint), func(key, b []byte) (bool, error) {
				if bytes.Compare(key[:len(top)], top) > 0 {
					return false, nil
				}

				batch.Delete(key)
				batch.Delete(leveldbProposalKey(valuehash.Bytes(b)))

				return true, nil
			}, true); err != nil {
			return false, e(err, "failed to find old proposals")
		}
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

func (db *TempPool) NewOperation(_ context.Context, operationhash util.Hash) (op base.Operation, found bool, _ error) {
	e := util.StringErrorFunc("failed to find operation")

	switch b, found, err := db.st.Get(leveldbNewOperationKey(operationhash)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &op); err != nil {
			return nil, false, e(err, "")
		}

		return op, true, nil
	}
}

func (db *TempPool) NewOperationHashes(
	ctx context.Context,
	limit uint64,
	filter func(operationhash, facthash util.Hash, _ isaac.PoolOperationHeader) (bool, error),
) ([]util.Hash, error) {
	e := util.StringErrorFunc("failed to find new operations")

	ops := make([]util.Hash, limit)
	var removes []util.Hash

	nfilter := filter
	if nfilter == nil {
		nfilter = func(operations, facthash util.Hash, _ isaac.PoolOperationHeader) (bool, error) { return true, nil }
	}

	var i uint64

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered),
		func(_ []byte, b []byte) (bool, error) {
			h, left := loadNewOperationHeader(b)

			var operationhash, facthash util.Hash

			switch keys := splitLeveldbJoinedKeys(left); {
			case len(keys) != 2: //nolint:gomnd //...
				return false, errors.Errorf("invalid operation ordered key")
			default:
				operationhash = valuehash.Bytes(keys[0])
				facthash = valuehash.Bytes(keys[1])
			}

			switch ok, err := nfilter(operationhash, facthash, h); {
			case err != nil:
				return false, err
			case !ok:
				removes = append(removes, facthash)

				return true, nil
			}

			ops[i] = operationhash
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

	key, orderedkey := newNewOperationLeveldbKeys(op.Hash(), facthash)

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

	h := newOperationHeader(op)

	batch.Put(key, b)
	batch.Put(orderedkey, util.ConcatBytesSlice(h[:], joinLeveldbKeys(op.Hash().Bytes(), facthash.Bytes())))

	batch.Put(leveldbNewOperationKeysKey(facthash), joinLeveldbKeys(key, orderedkey))

	if err := db.st.Write(batch, nil); err != nil {
		return false, e(err, "")
	}

	return true, nil
}

type PoolOperationHeader struct {
	version   byte
	addedAt   [10]byte
	hintBytes [289]byte
}

func (h PoolOperationHeader) Version() byte {
	return h.version
}

func (h PoolOperationHeader) AddedAt() []byte {
	return h.addedAt[:]
}

func (h PoolOperationHeader) HintBytes() []byte {
	return h.hintBytes[:]
}

func newOperationHeader(op base.Operation) [300]byte {
	b := [300]byte{}

	b[0] = 0x01 // NOTE header version(1)

	copy(b[1:11], util.Int64ToBytes(localtime.UTCNow().UnixNano())) // NOTE added timestamp(10)

	if i, ok := op.Fact().(hint.Hinter); ok {
		copy(b[11:], i.Hint().Bytes())
	}

	return b
}

func loadNewOperationHeader(b []byte) (header PoolOperationHeader, left []byte) {
	if len(b) < 300 { //nolint:gomnd //...
		return header, b
	}

	header.version = b[0]
	copy(header.addedAt[:], b[1:])
	copy(header.hintBytes[:], b[11:])

	return header, b[300:]
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

		return j[0].(base.INITVoteproof), j[1].(base.ACCEPTVoteproof), true, nil //nolint:forcetypeassert //...
	}
}

func (db *TempPool) SetLastVoteproofs(ivp base.INITVoteproof, avp base.ACCEPTVoteproof) error {
	e := util.StringErrorFunc("failed to set last voteproofs")

	if !ivp.Point().Point.Equal(avp.Point().Point) {
		return e(nil, "voteproofs should have same point")
	}

	if _, err := db.lastvoteproofs.Set(func(i interface{}) (interface{}, error) {
		var old [2]base.Voteproof
		if i != nil {
			old = i.([2]base.Voteproof) //nolint:forcetypeassert //...

			if ivp.Point().Compare(old[0].Point()) < 1 {
				return nil, util.ErrLockedSetIgnore.Call()
			}
		}

		vps := [2]base.Voteproof{ivp, avp}
		b, err := db.marshal(vps)
		if err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbKeyLastVoteproofs, b, nil); err != nil {
			return nil, err
		}

		return vps, nil
	}); err != nil {
		return e(err, "failed to set last voteproofs")
	}

	return nil
}

func (db *TempPool) removeNewOperations(ctx context.Context, facthashes []util.Hash) error {
	worker := util.NewErrgroupWorker(ctx, math.MaxInt8)
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
				return err
			case !found:
				return nil
			}

			switch keys := splitLeveldbJoinedKeys(b); {
			case len(keys) != 2: //nolint:gomnd //...
				return errors.Errorf("invalid joined key for operation")
			default:
				removekeysch <- infokey
				removekeysch <- keys[0]
				removekeysch <- keys[1]

				return nil
			}
		}); err != nil {
			break
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return err
	}

	close(removekeysch)
	<-donech

	return db.st.Write(batch, nil)
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

	if err := encoder.Decode(enc, u[0], &ivp); err != nil {
		return e(err, "")
	}

	var avp base.ACCEPTVoteproof

	if err := encoder.Decode(enc, u[1], &avp); err != nil {
		return e(err, "")
	}

	_ = db.lastvoteproofs.SetValue([2]base.Voteproof{ivp, avp})

	return nil
}

func newNewOperationLeveldbKeys(operationhash, facthash util.Hash) (key []byte, orderedkey []byte) {
	return leveldbNewOperationKey(operationhash), leveldbNewOperationOrderedKey(facthash)
}

func joinLeveldbKeys(a ...[]byte) []byte {
	return bytes.Join(a, leveldbKeysJoinSep)
}
