package database

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/localtime"
)

var (
	leveldbKeyPrefixSuffrage                = []byte{0x00, 0x01}
	leveldbKeyPrefixSuffrageHeight          = []byte{0x00, 0x02}
	leveldbKeyPrefixState                   = []byte{0x00, 0x03}
	leveldbKeyPrefixInStateOperation        = []byte{0x00, 0x04}
	leveldbKeyPrefixKnownOperation          = []byte{0x00, 0x05}
	leveldbKeyPrefixProposal                = []byte{0x00, 0x06}
	leveldbKeyPrefixProposalByPoint         = []byte{0x00, 0x07}
	leveldbKeyPrefixBlockDataMap            = []byte{0x00, 0x08}
	leveldbKeyPrefixNewOperation            = []byte{0x00, 0x09}
	leveldbKeyPrefixNewOperationOrdered     = []byte{0x00, 0x10}
	leveldbKeyPrefixNewOperationOrderedKeys = []byte{0x00, 0x11}

	leveldbNewOperationOrderedKeysJoinSep   = bytes.Repeat([]byte{0xff}, 10)
	leveldbNewOperationOrderedKeysJoinedSep = util.ConcatBytesSlice(
		leveldbNewOperationOrderedKeysJoinSep,
		leveldbKeyPrefixNewOperationOrdered,
	)
)

var (
	leveldbBeginSuffrageKey = util.ConcatBytesSlice(leveldbKeyPrefixSuffrage, []byte(strings.Repeat("0", 20)))
	leveldbSuffrageStateKey = leveldbStateKey(isaac.SuffrageStateKey)
)

type baseLeveldb struct {
	sync.Mutex
	*baseDatabase
	st leveldbstorage.ReadStorage
}

func newBaseLeveldb(
	st leveldbstorage.ReadStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseLeveldb {
	return &baseLeveldb{
		baseDatabase: newBaseDatabase(
			encs,
			enc,
		),
		st: st,
	}
}

func (db *baseLeveldb) Close() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close baseDatabase")
	}

	return nil
}

func (db *baseLeveldb) Remove() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close baseDatabase")
	}

	if err := db.st.Remove(); err != nil {
		return errors.Wrap(err, "failed to remove baseDatabase")
	}

	return nil
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

func (db *baseLeveldb) state(key string) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(leveldbStateKey(key)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		i, err := db.decodeState(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return i, true, nil
	}
}

func leveldbSuffrageKey(height base.Height) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixSuffrage, []byte(fmt.Sprintf("%021d", height)))
}

func leveldbSuffrageHeightKey(suffrageheight base.Height) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixSuffrageHeight, []byte(fmt.Sprintf("%021d", suffrageheight)))
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

func leveldbBlockDataMapKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixBlockDataMap,
		[]byte(fmt.Sprintf("%021d", height)),
	)
}

func leveldbNewOperationOrderedKey(facthash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixNewOperationOrdered,
		[]byte(localtime.RFC3339(localtime.UTCNow())),
		facthash.Bytes(),
	)
}

func leveldbNewOperationKeysKey(facthash util.Hash) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixNewOperationOrderedKeys,
		facthash.Bytes(),
	)
}

func leveldbNewOperationKey(facthash util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixNewOperation, facthash.Bytes())
}
