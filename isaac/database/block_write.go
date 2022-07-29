package isaacdatabase

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/syndtr/goleveldb/leveldb"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type LeveldbBlockWrite struct {
	*baseLeveldb
	mp         *util.Locked
	sufst      *util.Locked
	policy     *util.Locked
	proof      *util.Locked
	laststates *util.ShardedMap
	height     base.Height
	sync.Mutex
}

func NewLeveldbBlockWrite(
	height base.Height,
	st *leveldbstorage.Storage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *LeveldbBlockWrite {
	pst := leveldbstorage.NewPrefixStorage(st, newPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height))

	return &LeveldbBlockWrite{
		baseLeveldb: newBaseLeveldb(pst, encs, enc),
		height:      height,
		mp:          util.EmptyLocked(),
		sufst:       util.EmptyLocked(),
		policy:      util.EmptyLocked(),
		proof:       util.EmptyLocked(),
		laststates:  util.NewShardedMap(math.MaxInt8),
	}
}

func (db *LeveldbBlockWrite) Close() error {
	db.Lock()
	defer db.Unlock()

	if db.mp == nil {
		return nil
	}

	db.clean()

	return nil
}

func (db *LeveldbBlockWrite) Write() error {
	db.Lock()
	defer db.Unlock()

	db.laststates.Close()

	return nil
}

func (db *LeveldbBlockWrite) clean() {
	db.mp = nil
	db.sufst = nil
	db.policy = nil
	db.proof = nil

	if db.laststates != nil {
		db.laststates.Close()
		db.laststates = nil
	}
}

func (db *LeveldbBlockWrite) Cancel() error {
	db.Lock()
	defer db.Unlock()

	if db.mp == nil {
		return nil
	}

	db.clean()

	return nil
}

func (db *LeveldbBlockWrite) SetStates(sts []base.State) error {
	if len(sts) < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to set states in TempLeveldbDatabase")

	worker := util.NewErrgroupWorker(context.Background(), int64(len(sts)))
	defer worker.Close()

	for i := range sts {
		st := sts[i]

		if err := worker.NewJob(func(context.Context, uint64) error {
			if err := db.setState(st); err != nil {
				return err
			}

			ops := st.Operations()

			for j := range ops {
				op := ops[j]

				if err := db.st.Put(leveldbInStateOperationKey(op), op.Bytes(), nil); err != nil {
					return err
				}
			}

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), int64(len(ops)))
	defer worker.Close()

	e := util.StringErrorFunc("failed to set operation")

	for i := range ops {
		op := ops[i]
		if op == nil {
			return e(nil, "empty operation hash")
		}

		if err := worker.NewJob(func(context.Context, uint64) error {
			if err := db.st.Put(leveldbKnownOperationKey(op), op.Bytes(), nil); err != nil {
				return e(err, "")
			}

			return nil
		}); err != nil {
			return e(err, "")
		}
	}

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) BlockMap() (base.BlockMap, error) {
	switch i, _ := db.mp.Value(); {
	case i == nil:
		return nil, storage.NotFoundError.Errorf("empty blockmap")
	default:
		return i.(base.BlockMap), nil //nolint:forcetypeassert //...
	}
}

func (db *LeveldbBlockWrite) SetBlockMap(m base.BlockMap) error {
	if _, err := db.mp.Set(func(i interface{}) (interface{}, error) {
		b, err := db.marshal(m)
		if err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbBlockMapKey(m.Manifest().Height()), b, nil); err != nil {
			return nil, err
		}

		if i != nil && m.Manifest().Height() <= i.(base.BlockMap).Manifest().Height() { //nolint:forcetypeassert //...
			return i, nil
		}

		return m, nil
	}); err != nil {
		return errors.Wrap(err, "failed to set blockmap")
	}

	return nil
}

func (db *LeveldbBlockWrite) SuffrageState() base.State {
	i, _ := db.sufst.Value()
	if i == nil {
		return nil
	}

	return i.(base.State) //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) NetworkPolicy() base.NetworkPolicy {
	i, _ := db.policy.Value()
	if i == nil {
		return nil
	}

	return i.(base.State).Value().(base.NetworkPolicyStateValue).Policy() //nolint:forcetypeassert //...
}

func (db *LeveldbBlockWrite) SetSuffrageProof(proof base.SuffrageProof) error {
	if _, err := db.proof.Set(func(i interface{}) (interface{}, error) {
		b, err := db.marshal(proof)
		if err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbSuffrageProofKey(proof.SuffrageHeight()), b, nil); err != nil {
			return nil, err
		}

		if err := db.st.Put(leveldbSuffrageProofByBlockHeightKey(proof.Map().Manifest().Height()), b, nil); err != nil {
			return nil, err
		}

		if i != nil && proof.SuffrageHeight() <=
			i.(base.SuffrageProof).SuffrageHeight() { //nolint:forcetypeassert //...
			return i, nil
		}

		return proof, nil
	}); err != nil {
		return errors.Wrap(err, "failed to set SuffrageProof")
	}

	return nil
}

func (db *LeveldbBlockWrite) TempDatabase() (isaac.TempDatabase, error) {
	e := util.StringErrorFunc("failed to make TempDatabase from BlockWriteDatabase")

	temp, err := func() (isaac.TempDatabase, error) {
		db.Lock()
		defer db.Unlock()

		if _, err := db.BlockMap(); err != nil {
			return nil, err
		}

		temp, err := newTempLeveldbFromBlockWriteStorage(db)
		if err != nil {
			return nil, err
		}

		return temp, nil
	}()
	if err != nil {
		return nil, e(err, "")
	}

	if err := db.Close(); err != nil {
		return nil, e(err, "")
	}

	return temp, nil
}

func (db *LeveldbBlockWrite) setState(st base.State) error {
	e := util.StringErrorFunc("failed to set state")

	if !db.isLastStates(st) {
		return nil
	}

	b, err := db.marshal(st)
	if err != nil {
		return errors.Wrap(err, "failed to set state")
	}

	switch {
	case base.IsSuffrageNodesState(st) && st.Key() == isaac.SuffrageStateKey:
		db.updateLockedStates(st, db.sufst)
	case base.IsNetworkPolicyState(st) && st.Key() == isaac.NetworkPolicyStateKey:
		db.updateLockedStates(st, db.policy)
	}

	if err := db.st.Put(leveldbStateKey(st.Key()), b, nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (db *LeveldbBlockWrite) isLastStates(st base.State) bool {
	var islast bool
	_, _ = db.laststates.Set(st.Key(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) && st.Height() <= i.(base.Height) { //nolint:forcetypeassert //...
			return nil, errors.Errorf("old")
		}

		islast = true

		return st.Height(), nil
	})

	return islast
}

func (*LeveldbBlockWrite) updateLockedStates(st base.State, locked *util.Locked) {
	_, _ = locked.Set(func(i interface{}) (interface{}, error) {
		if i != nil && st.Height() <= i.(base.State).Height() { //nolint:forcetypeassert //...
			return i, nil
		}

		return st, nil
	})
}

var (
	prefixStoragePrefixByHeightLength int = valuehash.SHA256Size + 21 + util.ULIDLen // len(ULID.String())
	emptyULID                             = bytes.Repeat([]byte{0x00}, util.ULIDLen)
)

func newPrefixStoragePrefixByHeight(label []byte, height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbstorage.HashPrefix(label),
		[]byte(fmt.Sprintf("%021d", height)),
		[]byte(util.ULID().String()),
	)
}

func emptyPrefixStoragePrefixByHeight(label []byte, height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbstorage.HashPrefix(label),
		[]byte(fmt.Sprintf("%021d", height)),
		emptyULID,
	)
}

func prefixStoragePrefixFromKey(b []byte) ([]byte, error) {
	e := util.StringErrorFunc("failed to parse prefix of PrefixStorage from key")

	if len(b) < prefixStoragePrefixByHeightLength {
		return nil, e(nil, "wrong key of prefix storage prefix")
	}

	return b[:prefixStoragePrefixByHeightLength], nil
}

func HeightFromPrefixStoragePrefix(b []byte) (base.Height, error) {
	e := util.StringErrorFunc("failed to parse height from PrefixStoragePrefix")

	if len(b) < prefixStoragePrefixByHeightLength {
		return base.NilHeight, e(nil, "wrong key of prefix storage prefix")
	}

	s := b[valuehash.SHA256Size : valuehash.SHA256Size+21]

	d, err := strconv.ParseInt(string(s), 10, 64)
	if err != nil {
		return base.NilHeight, e(err, "")
	}

	return base.Height(d), nil
}

func cleanOneHeight(st *leveldbstorage.Storage, height base.Height, prefix []byte) error {
	e := util.StringErrorFunc("failed to clean one height")

	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := &leveldbutil.Range{
		Start: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height),
		Limit: prefix,
	}

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return e(err, "")
	}

	r = leveldbutil.BytesPrefix(prefix)
	r.Start = r.Limit
	r.Limit = emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height+1)

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return e(err, "")
	}

	return nil
}

func removeHeight(st *leveldbstorage.Storage, height base.Height) error { //nolint:deadcode,unused //...
	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := &leveldbutil.Range{
		Start: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height),
		Limit: emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height+1),
	}

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to remove height")
	}

	return nil
}

// removeHigherHeights removes all the BlockWrite labeled data higher than
// height; including height
func removeHigherHeights(st *leveldbstorage.Storage, height base.Height) error {
	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := leveldbutil.BytesPrefix(leveldbstorage.HashPrefix(leveldbLabelBlockWrite))
	r.Start = emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height)

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to remove higher heights")
	}

	return nil
}

// removeLowerHeights removes all the BlockWrite labeled data lower than height;
// except height
func removeLowerHeights(st *leveldbstorage.Storage, height base.Height) error {
	batch := &leveldb.Batch{}
	defer batch.Reset()

	r := leveldbutil.BytesPrefix(leveldbstorage.HashPrefix(leveldbLabelBlockWrite))
	r.Limit = emptyPrefixStoragePrefixByHeight(leveldbLabelBlockWrite, height)

	if _, err := leveldbstorage.BatchRemove(st, r, 333); err != nil { //nolint:gomnd //...
		return errors.WithMessage(err, "failed to remove lower heights")
	}

	return nil
}
