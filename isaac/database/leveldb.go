package isaacdatabase

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
	leveldbKeyPrefixBlockMap                = []byte{0x00, 0x08}
	leveldbKeyPrefixNewOperation            = []byte{0x00, 0x09}
	leveldbKeyPrefixNewOperationOrdered     = []byte{0x00, 0x10}
	leveldbKeyPrefixNewOperationOrderedKeys = []byte{0x00, 0x11}

	leveldbKeyLastVoteproofs = []byte{0x00, 0x12}

	leveldbNewOperationOrderedKeysJoinSep   = bytes.Repeat([]byte{0xff}, 10) //nolint:gomnd //...
	leveldbNewOperationOrderedKeysJoinedSep = util.ConcatBytesSlice(
		leveldbNewOperationOrderedKeysJoinSep,
		leveldbKeyPrefixNewOperationOrdered,
	)
)

var (
	leveldbBeginSuffrageKey = util.ConcatBytesSlice(leveldbKeyPrefixSuffrage, []byte(strings.Repeat("0", 21)))
	leveldbSuffrageStateKey = leveldbStateKey(isaac.SuffrageStateKey)
)

type baseLeveldb struct {
	st leveldbstorage.ReadStorage
	*baseDatabase
	sync.Mutex
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

func (db *baseLeveldb) loadNetworkPolicy() (base.NetworkPolicy, bool, error) {
	e := util.StringErrorFunc("failed to load suffrage state")

	b, found, err := db.st.Get(leveldbStateKey(isaac.NetworkPolicyStateKey))

	switch {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	}

	switch hinter, err := db.readHinter(b); {
	case err != nil:
		return nil, true, e(err, "")
	default:
		i, ok := hinter.(base.State)
		if !ok {
			return nil, true, e(nil, "not state: %T", hinter)
		}

		if !base.IsNetworkPolicyState(i) {
			return nil, true, e(nil, "not NetworkPolicy state: %T", i)
		}

		return i.Value().(base.NetworkPolicyStateValue).Policy(), true, nil //nolint:forcetypeassert //...
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

func leveldbBlockMapKey(height base.Height) []byte {
	return util.ConcatBytesSlice(
		leveldbKeyPrefixBlockMap,
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

func loadLeveldbNewOperationKeys(b []byte) (key []byte, orderedkey []byte, err error) {
	if b == nil {
		return nil, nil, nil
	}

	i := bytes.LastIndex(b, leveldbNewOperationOrderedKeysJoinedSep)
	if i < 0 {
		return nil, nil, errors.Errorf("unknown NewOperations info key format")
	}

	return b[:i], b[i+len(leveldbNewOperationOrderedKeysJoinSep):], nil
}
