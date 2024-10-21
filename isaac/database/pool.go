package isaacdatabase

import (
	"bytes"
	"context"
	"math"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type TempPool struct {
	*baseLeveldb
	*util.ContextDaemon
	whenNewOperationsremoved          func(int, error)
	lastvoteproofs                    *util.Locked[[2]base.Voteproof]
	opcache                           util.GCache[string, base.Operation]
	cleanRemovedNewOperationsInterval time.Duration
	cleanRemovedNewOperationsDeep     int
	cleanRemovedProposalDeep          int
	cleanRemovedBallotDeep            int
}

func NewTempPool(
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	opcachesize int,
) (*TempPool, error) {
	return newTempPool(st, encs, enc, opcachesize)
}

func newTempPool(
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
	opcachesize int,
) (*TempPool, error) {
	pst := leveldbstorage.NewPrefixStorage(st, leveldbLabelPool[:])

	var opcache util.GCache[string, base.Operation]
	if opcachesize > 0 {
		opcache = util.NewLRUGCache[string, base.Operation](opcachesize)
	}

	db := &TempPool{
		baseLeveldb:                       newBaseLeveldb(pst, encs, enc),
		lastvoteproofs:                    util.EmptyLocked[[2]base.Voteproof](),
		cleanRemovedNewOperationsInterval: time.Minute * 33, //nolint:mnd //...
		cleanRemovedNewOperationsDeep:     3,                //nolint:mnd //...
		cleanRemovedProposalDeep:          3,                //nolint:mnd //...
		cleanRemovedBallotDeep:            3,                //nolint:mnd //...
		whenNewOperationsremoved:          func(int, error) {},
		opcache:                           opcache,
	}

	db.ContextDaemon = util.NewContextDaemon(db.startClean)

	return db, nil
}

func (db *TempPool) Close() error {
	e := util.StringError("close TempPool")

	switch err := db.Stop(); {
	case err == nil:
	case errors.Is(err, util.ErrDaemonAlreadyStopped):
	default:
		return e.Wrap(err)
	}

	if err := db.baseLeveldb.Close(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *TempPool) Proposal(h util.Hash) (pr base.ProposalSignFact, found bool, _ error) {
	e := util.StringError("find proposal by hash")

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	var b []byte

	switch i, found, err := pst.Get(leveldbProposalKey(h)); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found, len(i) < 1:
		return nil, false, nil
	default:
		b = i
	}

	if err := ReadDecodeFrame(db.encs, b, &pr); err != nil {
		return nil, true, err
	}

	return pr, true, nil
}

func (db *TempPool) ProposalBytes(h util.Hash) (enchint string, meta, body []byte, found bool, _ error) {
	pst, err := db.st()
	if err != nil {
		return enchint, nil, nil, false, err
	}

	switch b, found, err := pst.Get(leveldbProposalKey(h)); {
	case err != nil:
		return "", nil, nil, false, err
	case !found:
		return "", nil, nil, false, nil
	default:
		enchint, _, rb, rerr := ReadFrame(b)
		if rerr != nil {
			return "", nil, nil, false, rerr
		}

		return enchint, nil, rb, true, nil
	}
}

func (db *TempPool) ProposalByPoint(
	point base.Point,
	proposer base.Address,
	previousBlock util.Hash,
) (base.ProposalSignFact, bool, error) {
	e := util.StringError("find proposal by point")

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	switch b, found, err := pst.Get(leveldbProposalPointKey(point, proposer, previousBlock)); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	default:
		pr, found, err := db.Proposal(valuehash.NewBytes(b))
		if err != nil {
			return nil, false, e.Wrap(err)
		}

		return pr, found, nil
	}
}

func (db *TempPool) SetProposal(pr base.ProposalSignFact) (bool, error) {
	e := util.StringError("put proposal")

	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return false, e.Wrap(err)
	default:
		pst = i
	}

	key := leveldbProposalKey(pr.Fact().Hash())

	switch found, err := pst.Exists(key); {
	case err != nil:
		return false, e.Wrap(err)
	case found:
		return false, nil
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	_, prb, err := EncodeFrame(db.enc, nil, pr)
	if err != nil {
		return false, e.WithMessage(err, "proposal")
	}

	batch.Put(leveldbProposalKey(pr.Fact().Hash()), prb)
	batch.Put(
		leveldbProposalPointKey(
			pr.ProposalFact().Point(),
			pr.ProposalFact().Proposer(),
			pr.ProposalFact().PreviousBlock(),
		),
		pr.Fact().Hash().Bytes(),
	)

	if err := pst.Batch(batch, nil); err != nil {
		return false, e.Wrap(err)
	}

	return true, nil
}

func (db *TempPool) Operation(_ context.Context, operationhash util.Hash) (op base.Operation, found bool, _ error) {
	e := util.StringError("operation")

	if i, found := db.opFromCache(operationhash); found {
		return i, found, nil
	}

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	var b []byte

	switch i, found, err := pst.Get(leveldbNewOperationKey(operationhash)); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case len(i) < 1:
		return nil, false, nil
	default:
		b = i
	}

	if err := ReadDecodeFrame(db.encs, b, &op); err != nil {
		return nil, true, e.Wrap(err)
	}

	db.setOpCache(op)

	return op, true, nil
}

func (db *TempPool) OperationBytes(_ context.Context, operationhash util.Hash) (
	enchint string, meta, body []byte, found bool, _ error,
) {
	pst, err := db.st()
	if err != nil {
		return "", nil, nil, false, err
	}

	switch b, found, err := pst.Get(leveldbNewOperationKey(operationhash)); {
	case err != nil:
		return "", nil, nil, false, err
	case !found:
		return "", nil, nil, false, nil
	default:
		enchint, _, rb, rerr := ReadFrame(b)
		if rerr != nil {
			return "", nil, nil, false, rerr
		}

		return enchint, nil, rb, true, nil
	}
}

func (db *TempPool) OperationHashes(
	ctx context.Context,
	height base.Height,
	limit uint64,
	filter func(isaac.PoolOperationRecordMeta) (bool, error),
) ([][2]util.Hash, error) {
	e := util.StringError("find new operations")

	pst, err := db.st()
	if err != nil {
		return nil, e.Wrap(err)
	}

	nfilter := filter
	if nfilter == nil {
		nfilter = func(isaac.PoolOperationRecordMeta) (bool, error) { return true, nil }
	}

	ops := make([][2]util.Hash, limit)
	removeordereds := make([][]byte, limit)
	removeops := make([]util.Hash, limit)

	var opsindex uint64
	var removeorderedsindex, removeopsindex uint64

	facts := map[string]uint64{}
	defer func() {
		clear(facts)
		facts = nil
	}()

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered[:]),
		func(k []byte, b []byte) (bool, error) {
			meta, err := ReadFrameHeaderOperation(b)
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

			// NOTE filter duplicated fact; last one will be selected
			if prev, found := facts[meta.Fact().String()]; found {
				removeops[removeopsindex] = meta.Operation()
				removeopsindex++

				nops := make([][2]util.Hash, len(ops))
				copy(nops, ops[:prev])
				copy(nops[prev:], ops[prev+1:])

				ops = nops

				opsindex--
			}

			ops[opsindex] = [2]util.Hash{meta.Operation(), meta.Fact()}
			facts[meta.Fact().String()] = opsindex
			opsindex++

			if opsindex == limit {
				return false, nil
			}

			return true, nil
		},
		true,
	); err != nil {
		return nil, e.Wrap(err)
	}

	if err := db.removeNewOperationOrdereds(removeordereds[:removeorderedsindex]); err != nil {
		return nil, e.Wrap(err)
	}

	if err := db.setRemoveNewOperations(ctx, height, removeops[:removeopsindex]); err != nil {
		return nil, e.Wrap(err)
	}

	return ops[:opsindex], nil
}

func (db *TempPool) TraverseOperationsBytes(
	ctx context.Context,
	offset []byte,
	f func(enchint string, meta FrameHeaderPoolOperation, body, offset []byte) (bool, error),
) error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	return pst.Iter(
		offsetRangeLeveldbOperationOrderedKey(offset),
		func(key, b []byte) (bool, error) {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}

			var meta FrameHeaderPoolOperation

			switch i, err := ReadFrameHeaderOperation(b); {
			case err != nil:
				return false, err
			default:
				meta = i
			}

			switch enchint, _, body, found, err := db.OperationBytes(ctx, meta.Operation()); {
			case err != nil:
				return false, err
			case !found:
				return false, storage.ErrNotFound.Errorf("operation not found by ordered key")
			default:
				of, err := offsetFromLeveldbOperationOrderedKey(key)
				if err != nil {
					return false, err
				}

				return f(enchint, meta, body, of)
			}
		},
		true,
	)
}

func (db *TempPool) SetOperation(_ context.Context, op base.Operation) (bool, error) {
	e := util.StringError("put operation")

	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return false, e.Wrap(err)
	default:
		pst = i
	}

	oph := op.Hash()

	key, orderedkey := newNewOperationLeveldbKeys(op.Hash())

	switch found, err := pst.Exists(key); {
	case err != nil:
		return false, e.Wrap(err)
	case found:
		return false, nil
	}

	_, opb, err := EncodeFrame(db.enc, nil, op)
	if err != nil {
		return false, e.WithMessage(err, "operation")
	}

	oprb, err := WriteFrameHeaderOperation(op)
	if err != nil {
		return false, e.WithMessage(err, "operation record")
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	batch.Put(key, opb)
	batch.Put(orderedkey, oprb)
	batch.Put(leveldbNewOperationKeysKey(oph), orderedkey)

	if err := pst.Batch(batch, nil); err != nil {
		return false, e.Wrap(err)
	}

	db.setOpCache(op)

	return true, nil
}

func (db *TempPool) SuffrageExpelOperation(
	height base.Height,
	node base.Address,
) (base.SuffrageExpelOperation, bool, error) {
	e := util.StringError("get SuffrageExpelOperation")

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	nodeb := node.Bytes()
	heighti := height.Int64()

	var enchint string
	var opb []byte

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageExpelOperation[:]), func(_, b []byte) (bool, error) {
			switch ht, r, left, err := ReadFrameHeaderSuffrageExpelOperation(b); {
			case err != nil:
				return false, err
			case !bytes.Equal(nodeb, r.Node()):
				return true, nil
			case r.End() < heighti, r.Start() > heighti:
				return false, nil
			default:
				enchint = ht
				opb = left

				return false, nil
			}
		}, false); err != nil {
		return nil, false, e.WithMessage(err, "find old proposals")
	}

	if opb == nil {
		return nil, false, nil
	}

	var op base.SuffrageExpelOperation

	if err := DecodeFrame(db.encs, enchint, opb, &op); err != nil {
		return nil, false, e.Wrap(err)
	}

	return op, true, nil
}

func (db *TempPool) SetSuffrageExpelOperation(op base.SuffrageExpelOperation) error {
	e := util.StringError("set SuffrageExpelOperation")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	opb, err := EncodeFrameSuffrageExpelOperation(db.enc, op)
	if err != nil {
		return e.Wrap(err)
	}

	if err := pst.Put(newSuffrageExpelOperationKey(op.ExpelFact()), opb, nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *TempPool) TraverseSuffrageExpelOperations(
	_ context.Context,
	height base.Height,
	callback isaac.SuffrageVoteFunc,
) error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	heighti := height.Int64()

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageExpelOperation[:]), func(_, b []byte) (bool, error) {
			var op base.SuffrageExpelOperation

			switch enchint, r, opb, err := ReadFrameHeaderSuffrageExpelOperation(b); {
			case err != nil:
				return false, err
			case r.End() < heighti, r.Start() > heighti:
				return false, nil
			default:
				if err := DecodeFrame(db.encs, enchint, opb, &op); err != nil {
					return false, err
				}

				return callback(op)
			}
		}, false); err != nil {
		return errors.WithMessage(err, "traverse SuffrageExpelOperations")
	}

	return nil
}

func (db *TempPool) RemoveSuffrageExpelOperationsByFact(facts []base.SuffrageExpelFact) error {
	e := util.StringError("remove SuffrageExpelOperations")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	for i := range facts {
		batch.Delete(newSuffrageExpelOperationKey(facts[i]))
	}

	if err := pst.Batch(batch, nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *TempPool) RemoveSuffrageExpelOperationsByHeight(height base.Height) error {
	e := util.StringError("remove SuffrageExpelOperations by height")

	pst, err := db.st()
	if err != nil {
		return e.Wrap(err)
	}

	heighti := height.Int64()

	batch := pst.NewBatch()
	defer batch.Reset()

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeySuffrageExpelOperation[:]), func(key, b []byte) (bool, error) {
			switch _, r, _, err := ReadFrameHeaderSuffrageExpelOperation(b); {
			case err != nil:
				return false, err
			case r.End() > heighti:
				return true, nil
			default:
				batch.Delete(key)

				return true, nil
			}
		}, false); err != nil {
		return e.Wrap(err)
	}

	if err := pst.Batch(batch, nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (db *TempPool) removeNewOperationOrdereds(keys [][]byte) error {
	if len(keys) < 1 {
		return nil
	}

	pst, err := db.st()
	if err != nil {
		return err
	}

	batch := pst.NewBatch()
	for i := range keys {
		batch.Delete(keys[i])
	}

	return pst.Batch(batch, nil)
}

func (db *TempPool) setRemoveNewOperations(ctx context.Context, height base.Height, operationhashes []util.Hash) error {
	if len(operationhashes) < 1 {
		return nil
	}

	pst, err := db.st()
	if err != nil {
		return err
	}

	worker, err := util.NewBaseJobWorker(ctx, math.MaxInt8)
	if err != nil {
		return err
	}

	defer worker.Close()

	batch := pst.NewBatch()
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
			switch orderedkey, found, err := pst.Get(infokey); {
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

	return pst.Batch(batch, nil)
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
	e := util.StringError("set last voteproofs")

	switch {
	case ivp == nil || avp == nil:
		_ = db.lastvoteproofs.EmptyValue()

		return nil
	case !ivp.Point().Point.Equal(avp.Point().Point):
		return e.Errorf("voteproofs should have same point")
	}

	if _, err := db.lastvoteproofs.Set(func(old [2]base.Voteproof, isempty bool) ([2]base.Voteproof, error) {
		if !isempty {
			if ivp.Point().Compare(old[0].Point()) < 1 {
				return [2]base.Voteproof{}, util.ErrLockedSetIgnore
			}
		}

		return [2]base.Voteproof{ivp, avp}, nil
	}); err != nil {
		return e.WithMessage(err, "set last voteproofs")
	}

	return nil
}

func (db *TempPool) Ballot(point base.Point, stage base.Stage, isSuffrageConfirm bool) (base.Ballot, bool, error) {
	e := util.StringError("find ballot")

	spoint := base.NewStagePoint(point, stage)
	if err := spoint.IsValid(nil); err != nil {
		return nil, false, e.Wrap(err)
	}

	pst, err := db.st()
	if err != nil {
		return nil, false, e.Wrap(err)
	}

	var bl base.Ballot

	switch b, found, err := pst.Get(leveldbBallotKey(spoint, isSuffrageConfirm)); {
	case err != nil:
		return nil, false, e.Wrap(err)
	case !found:
		return nil, false, nil
	case len(b) < 1:
		return nil, false, nil
	default:
		if err := ReadDecodeFrame(db.encs, b, &bl); err != nil {
			return nil, true, err
		}

		return bl, true, nil
	}
}

func (db *TempPool) SetBallot(bl base.Ballot) (bool, error) {
	e := util.StringError("put ballot")

	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return false, e.Wrap(err)
	default:
		pst = i
	}

	key := leveldbBallotKey(bl.Point(), isaac.IsSuffrageConfirmBallotFact(bl.SignFact().Fact()))

	var blb []byte

	switch found, err := pst.Exists(key); {
	case err != nil:
		return false, e.Wrap(err)
	case found:
		return false, nil
	default:
		_, b, err := EncodeFrame(db.enc, nil, bl)
		if err != nil {
			return false, err
		}

		blb = b
	}

	if err := pst.Put(key, blb, nil); err != nil {
		return false, e.Wrap(err)
	}

	return true, nil
}

func (db *TempPool) EmptyHeights(f func(base.Height) error) error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	return pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixEmptyHeight[:]),
		func(key, _ []byte) (bool, error) {
			height, err := heightFromKey(key, leveldbKeyPrefixEmptyHeight)
			if err != nil {
				return false, err
			}

			return true, f(height)
		},
		true,
	)
}

func (db *TempPool) AddEmptyHeight(height base.Height) (bool, error) {
	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return false, err
	default:
		pst = i
	}

	key := leveldbEmptyHeight(height)

	switch found, err := pst.Exists(key); {
	case err != nil, found:
		return false, err
	default:
		return true, pst.Put(key, nil, nil)
	}
}

func (db *TempPool) RemoveEmptyHeight(height base.Height) (bool, error) {
	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return false, err
	default:
		pst = i
	}

	key := leveldbEmptyHeight(height)

	switch found, err := pst.Exists(key); {
	case err != nil, !found:
		return false, err
	default:
		return true, pst.Delete(key, nil)
	}
}

func (db *TempPool) startClean(ctx context.Context) error {
	ticker := time.NewTicker(db.cleanRemovedNewOperationsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return errors.WithStack(ctx.Err())
		case <-ticker.C:
			if _, err := db.st(); err != nil {
				return err
			}

			removed, err := db.cleanRemovedNewOperations()
			if removed > 0 || err != nil {
				db.whenNewOperationsremoved(removed, err)
			}

			_, _ = db.cleanProposals()
			_, _ = db.cleanBallots()
		}
	}
}

func (db *TempPool) cleanRemovedNewOperations() (int, error) {
	pst, err := db.st()
	if err != nil {
		return 0, err
	}

	var removed int

	top := base.NilHeight

	_ = pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixRemovedNewOperation[:]),
		func(key, _ []byte) (bool, error) {
			i, err := heightFromKey(key, leveldbKeyPrefixRemovedNewOperation)
			if err != nil {
				return true, nil
			}

			if i > top {
				top = i
			}

			return false, nil
		},
		false,
	)

	if top-3 < base.GenesisHeight {
		return removed, nil
	}

	height := top

	for range make([]int, db.cleanRemovedNewOperationsDeep-1) {
		height = height.SafePrev()
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	r := &leveldbutil.Range{Limit: leveldbRemovedNewOperationPrefixWithHeight(height)}
	start := leveldbKeyPrefixRemovedNewOperation[:]

	for {
		r.Start = start

		_ = pst.Iter(
			r,
			func(key, b []byte) (bool, error) {
				if batch.Len() >= 333 { //nolint:mnd //...
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

		if err := pst.Batch(batch, nil); err != nil {
			break
		}

		batch.Reset()
	}

	return removed, nil
}

func (db *TempPool) cleanProposals() (int, error) {
	return db.cleanByHeight(
		leveldbKeyPrefixProposalByPoint,
		db.cleanRemovedProposalDeep,
		func(batch *leveldbstorage.PrefixStorageBatch, _ []byte, b []byte) {
			batch.Delete(leveldbProposalKey(valuehash.Bytes(b)))
		},
	)
}

func (db *TempPool) cleanBallots() (int, error) {
	return db.cleanByHeight(leveldbKeyPrefixBallot, db.cleanRemovedBallotDeep, nil)
}

func (db *TempPool) cleanByHeight(
	prefix leveldbstorage.KeyPrefix,
	deep int,
	keyf func(*leveldbstorage.PrefixStorageBatch, []byte, []byte),
) (int, error) {
	pst, err := db.st()
	if err != nil {
		return 0, err
	}

	top := base.NilHeight

	var keys [][3]interface{}

	_ = pst.Iter(
		leveldbutil.BytesPrefix(prefix[:]),
		func(key, b []byte) (bool, error) {
			i, err := heightFromKey(key, prefix)
			if err != nil {
				keys = append(keys, [3]interface{}{key, b, nil})

				return true, nil
			}

			if i > top {
				top = i
			}

			keys = append(keys, [3]interface{}{key, b, i})

			return true, nil
		},
		false,
	)

	height := top

	switch {
	case len(keys) < 1:
		return 0, nil
	case top-3 < base.GenesisHeight:
		return 0, nil
	default:
		for range make([]int, deep) {
			height = height.SafePrev()
		}
	}

	batch := pst.NewBatch()
	defer batch.Reset()

	var removed int

	for i := range keys {
		key, b, j := keys[i][0].([]byte), keys[i][1].([]byte), keys[i][2] //nolint:forcetypeassert //...
		if j != nil && j.(base.Height) > height {                         //nolint:forcetypeassert //...
			continue
		}

		batch.Delete(key)

		if keyf != nil {
			keyf(batch, key, b)
		}

		removed++
	}

	if batch.Len() < 1 {
		return removed, nil
	}

	return removed, pst.Batch(batch, nil)
}

func (db *TempPool) opFromCache(h util.Hash) (base.Operation, bool) {
	if db.opcache == nil {
		return nil, false
	}

	return db.opcache.Get(h.String())
}

func (db *TempPool) setOpCache(op base.Operation) {
	if db.opcache == nil {
		return
	}

	db.opcache.Set(op.Hash().String(), op, 0)
}

func newNewOperationLeveldbKeys(op util.Hash) (key, orderedkey []byte) {
	return leveldbNewOperationKey(op), leveldbNewOperationOrderedKey(op)
}

func newSuffrageExpelOperationKey(fact base.SuffrageExpelFact) []byte {
	return leveldbSuffrageExpelOperation(fact)
}
