package isaacdatabase

import (
	"bytes"
	"fmt"
	"strconv"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage2 "github.com/spikeekips/mitum/storage/leveldb2"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/localtime"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	leveldbLabelBlockWrite = []byte("block_write")
	leveldbLabelPermanent  = []byte("permanent")
	leveldbLabelPool       = []byte("pool")
	leveldbLabelSyncPool   = []byte("sync_pool")
)

var (
	leveldbKeyPrefixState                   = []byte{0x00, 0x01}
	leveldbKeyPrefixInStateOperation        = []byte{0x00, 0x02}
	leveldbKeyPrefixKnownOperation          = []byte{0x00, 0x03}
	leveldbKeyPrefixProposal                = []byte{0x00, 0x04}
	leveldbKeyPrefixProposalByPoint         = []byte{0x00, 0x05}
	leveldbKeyPrefixBlockMap                = []byte{0x00, 0x06}
	leveldbKeyPrefixNewOperation            = []byte{0x00, 0x07}
	leveldbKeyPrefixNewOperationOrdered     = []byte{0x00, 0x08}
	leveldbKeyPrefixNewOperationOrderedKeys = []byte{0x00, 0x09}
	leveldbKeyPrefixRemovedNewOperation     = []byte{0x00, 0x0a}
	leveldbKeyLastVoteproofs                = []byte{0x00, 0x0b}
	leveldbKeyTempSyncMap                   = []byte{0x00, 0x0c}
	leveldbKeySuffrageProof                 = []byte{0x00, 0x0d}
	leveldbKeySuffrageProofByBlockHeight    = []byte{0x00, 0x0e}

	leveldbKeysJoinSep = []byte("mitum-leveldb-sep")
)

type baseLeveldb struct {
	st *leveldbstorage2.PrefixStorage
	*baseDatabase
	sync.Mutex
}

func newBaseLeveldb(
	st *leveldbstorage2.PrefixStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseLeveldb {
	return &baseLeveldb{
		baseDatabase: newBaseDatabase(encs, enc),
		st:           st,
	}
}

func (db *baseLeveldb) Close() error {
	db.Lock()
	defer db.Unlock()

	if db.st == nil {
		return nil
	}

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close baseDatabase")
	}

	db.clean()

	return nil
}

func (db *baseLeveldb) clean() {
	db.st = nil
	db.encs = nil
	db.enc = nil
}

func (db *baseLeveldb) existsInStateOperation(h util.Hash) (bool, error) {
	switch found, err := db.st.Exists(leveldbInStateOperationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists instate operation")
	}
}

func (db *baseLeveldb) existsKnownOperation(h util.Hash) (bool, error) {
	switch found, err := db.st.Exists(leveldbKnownOperationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists known operation")
	}
}

func (db *baseLeveldb) state(key string) (st base.State, found bool, _ error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(leveldbStateKey(key)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		if err := db.readHinter(b, &st); err != nil {
			return nil, false, e(err, "")
		}

		return st, true, nil
	}
}

func (db *baseLeveldb) loadLastBlockMap() (base.BlockMap, error) {
	e := util.StringErrorFunc("failed to load last blockmap")

	var m base.BlockMap

	if err := db.st.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockMap),
		func(_, b []byte) (bool, error) {
			return false, db.readHinter(b, &m)
		},
		false,
	); err != nil {
		return nil, e(err, "")
	}

	return m, nil
}

func (db *baseLeveldb) loadNetworkPolicy() (base.NetworkPolicy, bool, error) {
	e := util.StringErrorFunc("failed to load suffrage state")

	b, found, err := db.st.Get(leveldbStateKey(isaac.NetworkPolicyStateKey))

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	}

	var st base.State
	if err := db.readHinter(b, &st); err != nil {
		return nil, true, e(err, "")
	}

	if !base.IsNetworkPolicyState(st) {
		return nil, true, e(nil, "not NetworkPolicy state")
	}

	return st.Value().(base.NetworkPolicyStateValue).Policy(), true, nil //nolint:forcetypeassert //...
}

func leveldbStateKey(key string) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixState, []byte(key))
}

func leveldbInStateOperationKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixInStateOperation, h.Bytes())
}

func leveldbKnownOperationKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixKnownOperation, h.Bytes())
}

func leveldbProposalKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixProposal, h.Bytes())
}

func leveldbProposalPointKey(point base.Point, proposer base.Address) []byte {
	var b []byte
	if proposer != nil {
		b = proposer.Bytes()
	}

	return util.ConcatBytesSlice(
		leveldbKeyPrefixProposalByPoint,
		[]byte(fmt.Sprintf("%021d-%021d", point.Height(), point.Round())),
		[]byte("-"),
		b,
	)
}

func leveldbBlockMapKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixBlockMap,
		[]byte(fmt.Sprintf("%021d", height)),
	)
}

func leveldbNewOperationOrderedKey(operationhash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixNewOperationOrdered,
		[]byte(util.RFC3339(localtime.UTCNow())),
		operationhash.Bytes(),
	)
}

func leveldbNewOperationKeysKey(operationhash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixNewOperationOrderedKeys,
		operationhash.Bytes(),
	)
}

func leveldbNewOperationKey(operationhash util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixNewOperation, operationhash.Bytes())
}

func leveldbRemovedNewOperationPrefixWithHeight(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixRemovedNewOperation,
		[]byte(fmt.Sprintf("%021d", height)),
	)
}

func leveldbRemovedNewOperationKey(height base.Height, operationhash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbRemovedNewOperationPrefixWithHeight(height),
		operationhash.Bytes(),
	)
}

func splitLeveldbJoinedKeys(b []byte) [][]byte {
	if b == nil {
		return nil
	}

	return bytes.SplitN(b, leveldbKeysJoinSep, -1)
}

func leveldbTempSyncMapKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyTempSyncMap,
		[]byte(fmt.Sprintf("%021d", height)),
	)
}

func leveldbSuffrageProofKey(suffrageheight base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeySuffrageProof,
		[]byte(fmt.Sprintf("%021d", suffrageheight)),
	)
}

func leveldbSuffrageProofByBlockHeightKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeySuffrageProofByBlockHeight,
		[]byte(fmt.Sprintf("%021d", height)),
	)
}

func heightFromleveldbKey(b, prefix []byte) (base.Height, error) {
	e := util.StringErrorFunc("failed to parse height from leveldbBlockMapKey")

	if len(b) < len(prefix)+21 {
		return base.NilHeight, e(nil, "too short")
	}

	d, err := strconv.ParseInt(string(b[len(prefix):len(prefix)+21]), 10, 64)
	if err != nil {
		return base.NilHeight, e(err, "")
	}

	return base.Height(d), nil
}
