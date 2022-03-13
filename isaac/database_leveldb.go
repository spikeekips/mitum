package isaac

import (
	"bytes"
	"fmt"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	keyPrefixManifest        = []byte{0x00, 0x00}
	keyPrefixSuffrage        = []byte{0x00, 0x01}
	keyPrefixSuffrageHeight  = []byte{0x00, 0x02}
	keyPrefixState           = []byte{0x00, 0x03}
	keyPrefixOperation       = []byte{0x00, 0x04}
	keyPrefixProposal        = []byte{0x00, 0x05}
	keyPrefixProposalByPoint = []byte{0x00, 0x06}
)

var (
	beginSuffrageDBKey       = util.ConcatBytesSlice(keyPrefixSuffrage, []byte(strings.Repeat("0", 20)))
	beginSuffrageHeightDBKey = util.ConcatBytesSlice(keyPrefixSuffrageHeight, []byte(strings.Repeat("0", 20)))
)

type baseLeveldbDatabase struct {
	sync.Mutex
	st   leveldbstorage.ReadStorage
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func newBaseLeveldbDatabase(
	st leveldbstorage.ReadStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseLeveldbDatabase {
	return &baseLeveldbDatabase{st: st, encs: encs, enc: enc}
}

func (db *baseLeveldbDatabase) Close() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close baseDatabase")
	}

	return nil
}

func (db *baseLeveldbDatabase) Remove() error {
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

func (db *baseLeveldbDatabase) loadHinter(b []byte) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	var ht hint.Hint
	ht, raw, err := db.loadHint(b)
	if err != nil {
		return nil, err
	}

	switch i := db.encs.Find(ht); {
	case i == nil:
		return nil, util.NotFoundError.Errorf("encoder not found for %q", ht)
	default:
		return i.(encoder.Encoder).Decode(raw)
	}
}

func (db *baseLeveldbDatabase) loadHint(b []byte) (hint.Hint, []byte, error) {
	if len(b) < hint.MaxHintLength {
		return hint.Hint{}, nil, errors.Errorf("none hinted string; too short")
	}

	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return hint.Hint{}, nil, err
	}

	return ht, b[hint.MaxHintLength:], nil
}

func (db *baseLeveldbDatabase) marshal(i interface{}) ([]byte, error) {
	b, err := db.enc.Marshal(i)
	if err != nil {
		return nil, err
	}

	return db.encodeWithEncoder(b), nil
}

func (db *baseLeveldbDatabase) encodeWithEncoder(b []byte) []byte {
	h := make([]byte, hint.MaxHintLength)
	copy(h, db.enc.Hint().Bytes())

	return util.ConcatBytesSlice(h, b)
}

func (db *baseLeveldbDatabase) state(key string) (base.State, bool, error) {
	e := util.StringErrorFunc("failed to get state")

	switch b, found, err := db.st.Get(stateDBKey(key)); {
	case err != nil:
		return nil, false, e(err, "")
	case !found:
		return nil, false, nil
	default:
		i, err := db.loadState(b)
		if err != nil {
			return nil, false, e(err, "")
		}

		return i, true, nil
	}
}

func (db *baseLeveldbDatabase) existsOperation(h util.Hash) (bool, error) {
	switch found, err := db.st.Exists(operationDBKey(h)); {
	case err == nil:
		return found, nil
	default:
		return false, errors.Wrap(err, "failed to check exists operation")
	}
}

func (db *baseLeveldbDatabase) loadState(b []byte) (base.State, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load state")

	hinter, err := db.loadHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, nil
	}

	switch i, ok := hinter.(base.State); {
	case !ok:
		return nil, e(nil, "not suffrage state: %T", hinter)
	default:
		return i, nil
	}
}

func (db *baseLeveldbDatabase) decodeManifest(b []byte) (base.Manifest, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load manifest")

	hinter, err := db.loadHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, e(nil, "empty manifest")
	}

	switch i, ok := hinter.(base.Manifest); {
	case !ok:
		return nil, e(nil, "not manifest: %T", hinter)
	default:
		return i, nil
	}
}

func (db *baseLeveldbDatabase) decodeSuffrage(b []byte) (base.State, error) {
	e := util.StringErrorFunc("failed to load suffrage")

	switch i, err := db.loadState(b); {
	case err != nil:
		return nil, e(err, "failed to load suffrage state")
	case i.Value() == nil:
		return nil, storage.NotFoundError.Errorf("state value not found")
	default:
		if _, ok := i.Value().(base.SuffrageStateValue); !ok {
			return nil, e(nil, "not suffrage state value: %T", i.Value())
		}

		return i, nil
	}
}

func manifestDBKey(height base.Height) []byte {
	return util.ConcatBytesSlice(keyPrefixState, []byte(fmt.Sprintf("%020d", height)))
}

func suffrageDBKey(height base.Height) []byte {
	return util.ConcatBytesSlice(keyPrefixSuffrage, []byte(fmt.Sprintf("%020d", height)))
}

func suffrageHeightDBKey(suffrageheight base.Height) []byte {
	return util.ConcatBytesSlice(keyPrefixSuffrageHeight, []byte(fmt.Sprintf("%020d", suffrageheight)))
}

func stateDBKey(key string) []byte {
	return util.ConcatBytesSlice(keyPrefixState, []byte(key))
}

func operationDBKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(keyPrefixOperation, h.Bytes())
}

func proposalDBKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(keyPrefixProposal, h.Bytes())
}

func proposalPointDBKey(point base.Point, proposer base.Address) []byte {
	var b []byte
	if proposer != nil {
		b = proposer.Bytes()
	}

	return util.ConcatBytesSlice(
		keyPrefixProposalByPoint,
		[]byte(fmt.Sprintf("%020d-%020d", point.Height(), point.Round())),
		[]byte("-"),
		b,
	)
}
