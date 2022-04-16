package database

import (
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var (
	leveldbKeyPrefixSuffrage        = []byte{0x00, 0x01}
	leveldbKeyPrefixSuffrageHeight  = []byte{0x00, 0x02}
	leveldbKeyPrefixState           = []byte{0x00, 0x03}
	leveldbKeyPrefixOperation       = []byte{0x00, 0x04}
	leveldbKeyPrefixProposal        = []byte{0x00, 0x05}
	leveldbKeyPrefixProposalByPoint = []byte{0x00, 0x06}
	leveldbKeyPrefixBlockDataMap    = []byte{0x00, 0x07}
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

func (db *baseLeveldb) existsOperation(h util.Hash) (bool, error) {
	switch found, err := db.st.Exists(leveldbOperationKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists operation")
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

func leveldbOperationKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(leveldbKeyPrefixOperation, h.Bytes())
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