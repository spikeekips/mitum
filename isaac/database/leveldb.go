package isaacdatabase

import (
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/localtime"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

var (
	leveldbLabelBlockWrite = leveldbstorage.KeyPrefix{0x01, 0x01}
	leveldbLabelPermanent  = leveldbstorage.KeyPrefix{0x01, 0x02}
	leveldbLabelPool       = leveldbstorage.KeyPrefix{0x01, 0x03}
	leveldbLabelSyncPool   = leveldbstorage.KeyPrefix{0x01, 0x04}
)

var (
	leveldbKeyPrefixState                   = leveldbstorage.KeyPrefix{0x02, 0x01}
	leveldbKeyPrefixInStateOperation        = leveldbstorage.KeyPrefix{0x02, 0x02}
	leveldbKeyPrefixKnownOperation          = leveldbstorage.KeyPrefix{0x02, 0x03}
	leveldbKeyPrefixProposal                = leveldbstorage.KeyPrefix{0x02, 0x04}
	leveldbKeyPrefixProposalByPoint         = leveldbstorage.KeyPrefix{0x02, 0x05}
	leveldbKeyPrefixBlockMap                = leveldbstorage.KeyPrefix{0x02, 0x06}
	leveldbKeyPrefixNewOperation            = leveldbstorage.KeyPrefix{0x02, 0x07}
	leveldbKeyPrefixNewOperationOrdered     = leveldbstorage.KeyPrefix{0x02, 0x08}
	leveldbKeyPrefixNewOperationOrderedKeys = leveldbstorage.KeyPrefix{0x02, 0x09}
	leveldbKeyPrefixRemovedNewOperation     = leveldbstorage.KeyPrefix{0x02, 0x0a}
	leveldbKeyTempSyncMap                   = leveldbstorage.KeyPrefix{0x02, 0x0c}
	leveldbKeySuffrageProof                 = leveldbstorage.KeyPrefix{0x02, 0x0d}
	leveldbKeySuffrageProofByBlockHeight    = leveldbstorage.KeyPrefix{0x02, 0x0e}
	leveldbKeySuffrageExpelOperation        = leveldbstorage.KeyPrefix{0x02, 0x0f}
	leveldbKeyTempMerged                    = leveldbstorage.KeyPrefix{0x02, 0x10}
	leveldbKeyPrefixBallot                  = leveldbstorage.KeyPrefix{0x02, 0x11}
	leveldbKeyPrefixEmptyHeight             = leveldbstorage.KeyPrefix{0x02, 0x12}
)

type baseLeveldb struct {
	encs *encoder.Encoders
	enc  encoder.Encoder
	pst  *leveldbstorage.PrefixStorage
	sync.RWMutex
}

func newBaseLeveldb(
	pst *leveldbstorage.PrefixStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseLeveldb {
	return &baseLeveldb{
		encs: encs,
		enc:  enc,
		pst:  pst,
	}
}

func (db *baseLeveldb) st() (*leveldbstorage.PrefixStorage, error) {
	db.RLock()
	defer db.RUnlock()

	if db.pst == nil {
		return nil, storage.ErrClosed.WithStack()
	}

	return db.pst, nil
}

func (db *baseLeveldb) Prefix() []byte {
	switch pst, err := db.st(); {
	case err != nil:
		return nil
	default:
		return pst.Prefix()
	}
}

func (db *baseLeveldb) Close() error {
	db.Lock()
	defer db.Unlock()

	if db.pst == nil {
		return nil
	}

	if err := db.pst.Close(); err != nil {
		return errors.Wrap(err, "close baseDatabase")
	}

	db.pst = nil
	db.encs = nil
	db.enc = nil

	return nil
}

func (db *baseLeveldb) Remove() error {
	pst, err := db.st()
	if err != nil {
		return err
	}

	return pst.Remove()
}

func (db *baseLeveldb) existsInStateOperation(h util.Hash) (bool, error) {
	pst, err := db.st()
	if err != nil {
		return false, err
	}

	switch found, err := pst.Exists(leveldbInStateOperationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "check exists instate operation")
	}
}

func (db *baseLeveldb) existsKnownOperation(h util.Hash) (bool, error) {
	pst, err := db.st()
	if err != nil {
		return false, err
	}

	switch found, err := pst.Exists(leveldbKnownOperationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "check exists known operation")
	}
}

func (db *baseLeveldb) loadLastBlockMap() (m base.BlockMap, enchint string, meta, body []byte, _ error) {
	e := util.StringError("load last blockmap")

	var pst *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return nil, enchint, nil, nil, e.Wrap(err)
	default:
		pst = i
	}

	if err := pst.Iter(
		leveldbutil.BytesPrefix(leveldbKeyPrefixBlockMap[:]),
		func(_, b []byte) (bool, error) {
			var err error

			enchint, meta, body, err = ReadOneHeaderFrame(b)
			if err != nil {
				return false, err
			}

			return false, DecodeFrame(db.encs, enchint, body, &m)
		},
		false,
	); err != nil {
		return nil, enchint, nil, nil, e.Wrap(err)
	}

	return m, enchint, meta, body, nil
}

func (db *baseLeveldb) loadNetworkPolicy() (base.State, base.NetworkPolicy, bool, error) {
	e := util.StringError("load suffrage state")

	pst, err := db.st()
	if err != nil {
		return nil, nil, false, e.Wrap(err)
	}

	b, found, err := pst.Get(leveldbStateKey(isaac.NetworkPolicyStateKey))

	switch {
	case err != nil:
		return nil, nil, false, e.Wrap(err)
	case !found:
		return nil, nil, false, nil
	case len(b) < 1:
		return nil, nil, false, nil
	}

	var st base.State

	if err := ReadDecodeFrame(db.encs, b, &st); err != nil {
		return nil, nil, true, err
	}

	if !base.IsNetworkPolicyState(st) {
		return nil, nil, true, e.Errorf("not NetworkPolicy state")
	}

	return st, st.Value().(base.NetworkPolicyStateValue).Policy(), true, nil //nolint:forcetypeassert //...
}

func leveldbStateKey(key string) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixState, []byte(key))
}

func leveldbInStateOperationKey(h util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixInStateOperation, h.Bytes())
}

func leveldbKnownOperationKey(h util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixKnownOperation, h.Bytes())
}

func leveldbProposalKey(h util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixProposal, h.Bytes())
}

func leveldbProposalPointKey(point base.Point, proposer base.Address, previousBlock util.Hash) []byte {
	var pb, bb []byte
	if proposer != nil {
		pb = proposer.Bytes()
	}

	if previousBlock != nil {
		bb = previousBlock.Bytes()
	}

	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixProposalByPoint,
		point.Bytes(),
		[]byte("-"),
		pb,
		bb,
	)
}

func leveldbBlockMapKey(height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixBlockMap,
		height.Bytes(),
	)
}

func leveldbNewOperationOrderedKey(operationhash util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixNewOperationOrdered,
		util.Int64ToBytes(localtime.Now().UnixNano()),
		operationhash.Bytes(),
	)
}

func leveldbNewOperationOrderedKeyPrefix(prefix []byte) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixNewOperationOrdered,
		prefix,
	)
}

func leveldbNewOperationKeysKey(operationhash util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixNewOperationOrderedKeys,
		operationhash.Bytes(),
	)
}

func leveldbNewOperationKey(operationhash util.Hash) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixNewOperation, operationhash.Bytes())
}

func leveldbRemovedNewOperationPrefixWithHeight(height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyPrefixRemovedNewOperation,
		height.Bytes(),
	)
}

func leveldbRemovedNewOperationKey(height base.Height, operationhash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbRemovedNewOperationPrefixWithHeight(height),
		operationhash.Bytes(),
	)
}

func leveldbTempSyncMapKey(height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyTempSyncMap,
		height.Bytes(),
	)
}

func leveldbSuffrageProofKey(suffrageheight base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeySuffrageProof,
		suffrageheight.Bytes(),
	)
}

func leveldbSuffrageProofByBlockHeightKey(height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeySuffrageProofByBlockHeight,
		height.Bytes(),
	)
}

func leveldbSuffrageExpelOperation(fact base.SuffrageExpelFact) []byte {
	return leveldbstorage.NewPrefixKey(leveldbKeySuffrageExpelOperation, fact.ExpelEnd().Bytes(), fact.Hash().Bytes())
}

func leveldbBallotKey(point base.StagePoint, isSuffrageConfirm bool) []byte { // revive:disable-line:flag-parameter
	s := []byte("-")
	if isSuffrageConfirm {
		s = []byte("+")
	}

	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixBallot, point.Bytes(), s)
}

func leveldbEmptyHeight(height base.Height) []byte { // revive:disable-line:flag-parameter
	return leveldbstorage.NewPrefixKey(leveldbKeyPrefixEmptyHeight, height.Bytes())
}

func heightFromKey(b []byte, prefix leveldbstorage.KeyPrefix) (base.Height, error) {
	e := util.StringError("parse height from key")

	if len(b) < len(prefix)+8 {
		return base.NilHeight, e.Errorf("too short")
	}

	h, err := base.ParseHeightBytes(b[len(prefix) : len(prefix)+8])
	if err != nil {
		return base.NilHeight, e.Wrap(err)
	}

	return h, nil
}

func leveldbTempMergedKey(height base.Height) []byte {
	return leveldbstorage.NewPrefixKey(
		leveldbKeyTempMerged,
		height.Bytes(),
	)
}

func offsetFromLeveldbOperationOrderedKey(b []byte) ([]byte, error) {
	switch l := len(leveldbKeyPrefixNewOperationOrdered); {
	case len(b) <= l:
		return nil, errors.Errorf("not enough")
	default:
		return b[l : l+8], nil
	}
}

func offsetRangeLeveldbOperationOrderedKey(offset []byte) *leveldbutil.Range {
	r := leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered[:])

	if offset == nil {
		return r
	}

	start := leveldbutil.BytesPrefix(leveldbNewOperationOrderedKeyPrefix(offset)).Limit

	limit := make([]byte, len(start))
	copy(limit, leveldbutil.BytesPrefix(leveldbKeyPrefixNewOperationOrdered[:]).Limit)

	r = &leveldbutil.Range{
		Start: start,
		Limit: limit,
	}

	return r
}

func stateKeyFromKey(b []byte) (string, error) {
	l := len(leveldbKeyPrefixState)

	if len(b) < l+1 {
		return "", errors.Errorf("too short")
	}

	return string(b[l:]), nil
}

func AllLabelKeys() map[leveldbstorage.KeyPrefix]string {
	return map[leveldbstorage.KeyPrefix]string{
		leveldbLabelBlockWrite: "block_write",
		leveldbLabelPermanent:  "permanent",
		leveldbLabelPool:       "pool",
		leveldbLabelSyncPool:   "sync_pool",
	}
}

func AllPrefixKeys() map[leveldbstorage.KeyPrefix]string {
	return map[leveldbstorage.KeyPrefix]string{
		leveldbKeyPrefixState:                   "state",
		leveldbKeyPrefixInStateOperation:        "in_state_operation",
		leveldbKeyPrefixKnownOperation:          "known_operation",
		leveldbKeyPrefixProposal:                "proposal",
		leveldbKeyPrefixProposalByPoint:         "proposal_by_point",
		leveldbKeyPrefixBlockMap:                "blockmap",
		leveldbKeyPrefixNewOperation:            "new_operation",
		leveldbKeyPrefixNewOperationOrdered:     "new_operation_ordered",
		leveldbKeyPrefixNewOperationOrderedKeys: "new_operation_ordered_keys",
		leveldbKeyPrefixRemovedNewOperation:     "removed_new_operation",
		leveldbKeyTempSyncMap:                   "temp_sync_map",
		leveldbKeySuffrageProof:                 "suffrage_proof",
		leveldbKeySuffrageProofByBlockHeight:    "suffrage_proof_by_block_height",
		leveldbKeySuffrageExpelOperation:        "suffrage_expel_operation",
		leveldbKeyTempMerged:                    "temp_merged",
		leveldbKeyPrefixBallot:                  "ballot",
	}
}
