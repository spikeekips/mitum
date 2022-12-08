package isaacdatabase

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempPool struct {
	*baseLeveldb
	*util.ContextDaemon
	whenNewOperationsremoved          func(int, error)
	lastvoteproofs                    *util.Locked[[2]base.Voteproof]
	cleanRemovedNewOperationsInterval time.Duration
	cleanRemovedNewOperationsDeep     base.Height
}

func NewTempPool(st *leveldbstorage.Storage, encs *encoder.Encoders, enc encoder.Encoder) (*TempPool, error) {
	return newTempPool(st, encs, enc)
}

func newTempPool(st *leveldbstorage.Storage, encs *encoder.Encoders, enc encoder.Encoder) (*TempPool, error) {
	pst := leveldbstorage.NewPrefixStorage(st, leveldbLabelPool)

	db := &TempPool{
		baseLeveldb:                       newBaseLeveldb(pst, encs, enc),
		lastvoteproofs:                    util.EmptyLocked([2]base.Voteproof{}),
		cleanRemovedNewOperationsInterval: time.Minute * 33, //nolint:gomnd //...
		cleanRemovedNewOperationsDeep:     3,                //nolint:gomnd //...
		whenNewOperationsremoved:          func(int, error) {},
	}

	db.ContextDaemon = util.NewContextDaemon(db.startClean)

	return db, nil
}

func (db *TempPool) Close() error {
	e := util.StringErrorFunc("failed to close TempPool")

	switch err := db.Stop(); {
	case err == nil:
	case errors.Is(err, util.ErrDaemonAlreadyStopped):
	default:
		return e(err, "")
	}

	if err := db.baseLeveldb.Close(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *TempPool) Proposal(h util.Hash) (pr base.ProposalSignFact, found bool, _ error) {
	e := util.StringErrorFunc("failed to find proposal by hash")

	switch b, found, err := db.st.Get(leveldbProposalKey(h)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	case len(b) < 1:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &pr); err != nil {
			return nil, false, e(err, "")
		}

		return pr, true, nil
	}
}

func (db *TempPool) ProposalBytes(h util.Hash) (enchint hint.Hint, meta, body []byte, found bool, _ error) {
	return db.getRecordBytes(leveldbProposalKey(h), db.st.Get)
}

func (db *TempPool) ProposalByPoint(point base.Point, proposer base.Address) (base.ProposalSignFact, bool, error) {
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

func (db *TempPool) SetProposal(pr base.ProposalSignFact) (bool, error) {
	e := util.StringErrorFunc("failed to put proposal")

	key := leveldbProposalKey(pr.Fact().Hash())

	switch found, err := db.st.Exists(key); {
	case err != nil:
		return false, e(err, "")
	case found:
		return false, nil
	}

	batch := db.st.NewBatch()
	defer batch.Reset()

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

	b, _, err := db.marshal(pr, nil)
	if err != nil {
		return false, e(err, "failed to marshal proposal")
	}

	batch.Put(leveldbProposalKey(pr.Fact().Hash()), b)
	batch.Put(
		leveldbProposalPointKey(pr.ProposalFact().Point(), pr.ProposalFact().Proposer()),
		pr.Fact().Hash().Bytes(),
	)

	if err := db.st.Batch(batch, nil); err != nil {
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
	case len(b) < 1:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &op); err != nil {
			return nil, false, e(err, "")
		}

		return op, true, nil
	}
}

func (db *TempPool) NewOperationBytes(_ context.Context, operationhash util.Hash) (
	enchint hint.Hint, meta, body []byte, found bool, _ error,
) {
	return db.getRecordBytes(leveldbNewOperationKey(operationhash), db.st.Get)
}

func (db *TempPool) NewOperationHashes(
	ctx context.Context,
	height base.Height,
	limit uint64,
	filter func(isaac.PoolOperationRecordMeta) (bool, error),
) ([]util.Hash, error) {
	e := util.StringErrorFunc("failed to find new operations")

	nfilter := filter
	if nfilter == nil {
		nfilter = func(isaac.PoolOperationRecordMeta) (bool, error) { return true, nil }
	}

	ops := make([]util.Hash, limit)
	removeordereds := make([][]byte, limit)
	removeops := make([]util.Hash, limit)

	var opsindex uint64
	var removeorderedsindex, removeopsindex uint64

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered),
		func(k []byte, b []byte) (bool, error) {
			meta, err := ReadPoolOperationRecordMeta(b)
			if err != nil {
				removeordereds[removeorderedsindex] = k
				removeorderedsindex++

				return true, nil
			}

			switch ok, err := nfilter(meta); {
			case err != nil:
				return false, err
			case !ok:
				removeops[removeopsindex] = meta.Operation()
				removeopsindex++

				return true, nil
			}

			ops[opsindex] = meta.Operation()
			opsindex++

			if opsindex == limit {
				return false, nil
			}

			return true, nil
		},
		true,
	); err != nil {
		return nil, e(err, "")
	}

	if err := db.removeNewOperationOrdereds(removeordereds[:removeorderedsindex]); err != nil {
		return nil, e(err, "")
	}

	if err := db.setRemoveNewOperations(ctx, height, removeops[:removeopsindex]); err != nil {
		return nil, e(err, "")
	}

	return ops[:opsindex], nil
}

func (db *TempPool) SetNewOperation(_ context.Context, op base.Operation) (bool, error) {
	e := util.StringErrorFunc("failed to put operation")

	oph := op.Hash()

	key, orderedkey := newNewOperationLeveldbKeys(op.Hash())

	switch found, err := db.st.Exists(key); {
	case err != nil:
		return false, e(err, "")
	case found:
		return false, nil
	}

	b, _, err := db.marshal(op, nil)
	if err != nil {
		return false, e(err, "failed to marshal operation")
	}

	batch := db.st.NewBatch()
	defer batch.Reset()

	batch.Put(key, b)

	batch.Put(orderedkey, NewPoolOperationRecordMeta(op).Bytes())
	batch.Put(leveldbNewOperationKeysKey(oph), orderedkey)

	if err := db.st.Batch(batch, nil); err != nil {
		return false, e(err, "")
	}

	return true, nil
}

func (db *TempPool) SuffrageWithdrawOperation(
	height base.Height,
	node base.Address,
) (base.SuffrageWithdrawOperation, bool, error) {
	e := util.StringErrorFunc("failed to get SuffrageWithdrawOperation")

	nodeb := node.Bytes()

	var opb []byte

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageWithdrawOperation), func(_, b []byte) (bool, error) {
			switch rnodeb, start, end, left, err := db.readSuffrageWithdrawOperationsRecordMeta(b); {
			case err != nil:
				return false, err
			case !bytes.Equal(nodeb, rnodeb):
				return true, nil
			case end < height, start > height:
				return false, nil
			default:
				opb = left

				return false, nil
			}
		}, false); err != nil {
		return nil, false, e(err, "failed to find old proposals")
	}

	if opb == nil {
		return nil, false, nil
	}

	var op base.SuffrageWithdrawOperation

	if err := db.readHinter(opb, &op); err != nil {
		return nil, false, e(err, "")
	}

	return op, true, nil
}

func (db *TempPool) SetSuffrageWithdrawOperation(op base.SuffrageWithdrawOperation) error {
	e := util.StringErrorFunc("failed to set SuffrageWithdrawOperation")

	b, _, err := db.marshal(op, nil)
	if err != nil {
		return e(err, "failed to marshal")
	}

	fact := op.WithdrawFact()

	lb, err := util.NewLengthedBytesSlice(0x01, [][]byte{ //nolint:gomnd //...
		fact.Node().Bytes(),
		fact.WithdrawStart().Bytes(),
		fact.WithdrawEnd().Bytes(),
	})
	if err != nil {
		return e(err, "failed to marshal")
	}

	if err := db.st.Put(
		newSuffrageWithdrawOperationKey(op.WithdrawFact()),
		util.ConcatBytesSlice(lb, b),
		nil,
	); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *TempPool) TraverseSuffrageWithdrawOperations(
	_ context.Context,
	height base.Height,
	callback isaac.SuffrageVoteFunc,
) error {
	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageWithdrawOperation), func(key, b []byte) (bool, error) {
			var op base.SuffrageWithdrawOperation

			switch _, start, end, left, err := db.readSuffrageWithdrawOperationsRecordMeta(b); {
			case err != nil:
				return false, err
			case end < height, start > height:
				return false, nil
			default:
				if err := db.readHinter(left, &op); err != nil {
					return false, err
				}
			}

			return callback(op)
		}, false); err != nil {
		return errors.WithMessage(err, "failed to traverse SuffrageWithdrawOperations")
	}

	return nil
}

func (db *TempPool) RemoveSuffrageWithdrawOperationsByFact(facts []base.SuffrageWithdrawFact) error {
	e := util.StringErrorFunc("failed to remove SuffrageWithdrawOperations")

	batch := db.st.NewBatch()
	defer batch.Reset()

	for i := range facts {
		batch.Delete(newSuffrageWithdrawOperationKey(facts[i]))
	}

	if err := db.st.Batch(batch, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *TempPool) RemoveSuffrageWithdrawOperationsByHeight(height base.Height) error {
	e := util.StringErrorFunc("failed to remove SuffrageWithdrawOperations by height")

	batch := db.st.NewBatch()
	defer batch.Reset()

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageWithdrawOperation), func(key, b []byte) (bool, error) {
			switch _, _, end, _, err := db.readSuffrageWithdrawOperationsRecordMeta(b); {
			case err != nil:
				return false, err
			case end > height:
				return true, nil
			default:
				batch.Delete(key)

				return true, nil
			}
		}, false); err != nil {
		return e(err, "")
	}

	if err := db.st.Batch(batch, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *TempPool) removeNewOperationOrdereds(keys [][]byte) error {
	if len(keys) < 1 {
		return nil
	}

	batch := db.st.NewBatch()
	for i := range keys {
		batch.Delete(keys[i])
	}

	return db.st.Batch(batch, nil)
}

func (db *TempPool) setRemoveNewOperations(ctx context.Context, height base.Height, operationhashes []util.Hash) error {
	if len(operationhashes) < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(ctx, math.MaxInt8)
	defer worker.Close()

	batch := db.st.NewBatch()
	defer batch.Reset()

	batchch := make(chan func(bt *leveldbstorage.PrefixStorageBatch))
	donech := make(chan struct{})

	go func() {
		for i := range batchch {
			i(batch)
		}

		donech <- struct{}{}
	}()

	for i := range operationhashes {
		h := operationhashes[i]
		if h == nil {
			break
		}

		if err := worker.NewJob(func(context.Context, uint64) error {
			infokey := leveldbNewOperationKeysKey(h)
			switch orderedkey, found, err := db.st.Get(infokey); {
			case err != nil:
				return err
			case !found:
				return nil
			default:
				batchch <- func(bt *leveldbstorage.PrefixStorageBatch) {
					bt.Delete(infokey)
					bt.Delete(orderedkey)
					bt.Put(leveldbRemovedNewOperationKey(height, h), h.Bytes())
				}

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

	close(batchch)
	<-donech

	if batch.Len() < 1 {
		return nil
	}

	return db.st.Batch(batch, nil)
}

func (db *TempPool) LastVoteproofs() (base.INITVoteproof, base.ACCEPTVoteproof, bool, error) {
	switch i, isempty := db.lastvoteproofs.Value(); {
	case isempty:
		return nil, nil, false, nil
	default:
		return i[0].(base.INITVoteproof), i[1].(base.ACCEPTVoteproof), true, nil //nolint:forcetypeassert //...
	}
}

func (db *TempPool) SetLastVoteproofs(ivp base.INITVoteproof, avp base.ACCEPTVoteproof) error {
	e := util.StringErrorFunc("failed to set last voteproofs")

	switch {
	case ivp == nil || avp == nil:
		_ = db.lastvoteproofs.EmptyValue()

		return nil
	case !ivp.Point().Point.Equal(avp.Point().Point):
		return e(nil, "voteproofs should have same point")
	}

	if _, err := db.lastvoteproofs.Set(func(old [2]base.Voteproof, isempty bool) ([2]base.Voteproof, error) {
		if !isempty {
			if ivp.Point().Compare(old[0].Point()) < 1 {
				return [2]base.Voteproof{}, util.ErrLockedSetIgnore.Call()
			}
		}

		return [2]base.Voteproof{ivp, avp}, nil
	}); err != nil {
		return e(err, "failed to set last voteproofs")
	}

	return nil
}

func (db *TempPool) startClean(ctx context.Context) error {
	if db.st == nil {
		return errors.Errorf("already closed")
	}

	ticker := time.NewTicker(db.cleanRemovedNewOperationsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			removed, err := db.cleanRemovedNewOperations()
			if removed > 0 || err != nil {
				db.whenNewOperationsremoved(removed, err)
			}
		}
	}
}

func (db *TempPool) cleanRemovedNewOperations() (int, error) {
	var removed int

	top := base.NilHeight

	_ = db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixRemovedNewOperation),
		func(key, _ []byte) (bool, error) {
			i, err := heightFromleveldbKey(key, leveldbKeyPrefixRemovedNewOperation)
			if err != nil {
				return true, nil
			}

			top = i

			return false, nil
		},
		false,
	)

	if top-3 < base.GenesisHeight {
		return removed, nil
	}

	height := top

	for range make([]int, db.cleanRemovedNewOperationsDeep.Int64()-1) {
		height = height.SafePrev()
	}

	batch := db.st.NewBatch()
	defer batch.Reset()

	r := &leveldbutil.Range{Limit: leveldbRemovedNewOperationPrefixWithHeight(height)}
	start := leveldbKeyPrefixRemovedNewOperation

	for {
		r.Start = start

		_ = db.st.Iter(
			r,
			func(key, b []byte) (bool, error) {
				if batch.Len() >= 333 { //nolint:gomnd //...
					start = key

					return false, nil
				}

				batch.Delete(key)
				batch.Delete(leveldbNewOperationKey(valuehash.NewBytes(b)))
				removed++

				return false, nil
			},
			true,
		)

		if batch.Len() < 1 {
			break
		}

		if err := db.st.Batch(batch, nil); err != nil {
			break
		}

		batch.Reset()
	}

	return removed, nil
}

func (*TempPool) readSuffrageWithdrawOperationsRecordMeta(b []byte) (
	node []byte, start, end base.Height,
	left []byte,
	_ error,
) {
	_, r, left, err := util.ReadLengthedBytesSlice(b)

	switch {
	case err != nil:
		return nil, start, end, nil, err
	case len(r) < 3: //nolint:gomnd //...
		return nil, start, end, nil, errors.Errorf("missing record meta")
	case len(r[0]) < 1:
		return nil, start, end, nil, errors.Errorf("wrong format; empty node")
	default:
		s, err := base.ParseHeightBytes(r[1])
		if err != nil {
			return nil, start, end, nil, errors.WithMessage(err, "wrong start height")
		}

		e, err := base.ParseHeightBytes(r[2])
		if err != nil {
			return nil, start, end, nil, errors.WithMessage(err, "wrong end height")
		}

		return r[0], s, e, left, nil
	}
}

func newNewOperationLeveldbKeys(op util.Hash) (key []byte, orderedkey []byte) {
	return leveldbNewOperationKey(op), leveldbNewOperationOrderedKey(op)
}

func newSuffrageWithdrawOperationKey(fact base.SuffrageWithdrawFact) []byte {
	return leveldbSuffrageWithdrawOperation(fact)
}

type PoolOperationRecordMeta struct {
	addedAt  time.Time
	ophash   util.Hash
	facthash util.Hash
	ht       hint.Hint
	version  byte
}

func NewPoolOperationRecordMeta(op base.Operation) util.Byter {
	var htb []byte
	if i, ok := op.Fact().(hint.Hinter); ok {
		htb = i.Hint().Bytes()
	}

	b, _ := util.NewLengthedBytesSlice(0x01, [][]byte{ //nolint:gomnd //...
		util.Int64ToBytes(localtime.Now().UTC().UnixNano()), // NOTE added UTC timestamp(10)
		htb,
		op.Hash().Bytes(),
		op.Fact().Hash().Bytes(),
	}) //nolint:gomnd //...

	return util.BytesToByter(b)
}

func ReadPoolOperationRecordMeta(b []byte) (meta PoolOperationRecordMeta, _ error) {
	e := util.StringErrorFunc("failed to read pool operation record meta")

	var m [][]byte

	switch v, i, _, err := util.ReadLengthedBytesSlice(b); {
	case err != nil:
		return meta, e(err, "")
	case len(i) != 4: //nolint:gomnd //...
		return meta, e(nil, "wrong pool operation meta")
	default:
		meta.version = v
		m = i
	}

	nsec, err := util.BytesToInt64(m[0])
	if err != nil {
		return meta, e(nil, "wrong added at time")
	}

	meta.addedAt = time.Unix(0, nsec)

	meta.ht, err = hint.ParseHint(string(m[1]))
	if err != nil {
		return meta, e(nil, "wrong hint")
	}

	meta.ophash = valuehash.Bytes(m[2])
	meta.facthash = valuehash.Bytes(m[3])

	return meta, nil
}

func (h PoolOperationRecordMeta) Version() byte {
	return h.version
}

func (h PoolOperationRecordMeta) AddedAt() time.Time {
	return h.addedAt
}

func (h PoolOperationRecordMeta) Hint() hint.Hint {
	return h.ht
}

func (h PoolOperationRecordMeta) Operation() util.Hash {
	return h.ophash
}

func (h PoolOperationRecordMeta) Fact() util.Hash {
	return h.facthash
}
