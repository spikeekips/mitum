package isaac

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	keyPrefixManifest        = []byte{0x00, 0x00}
	keyPrefixSuffrage        = []byte{0x00, 0x01}
	keyPrefixState           = []byte{0x00, 0x02}
	keyPrefixOperation       = []byte{0x00, 0x03}
	keyPrefixProposal        = []byte{0x00, 0x04}
	keyPrefixProposalByPoint = []byte{0x00, 0x05}
)

type baseDatabase struct {
	sync.Mutex
	st   leveldbstorage.ReadStorage
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func newBaseDatabase(
	st leveldbstorage.ReadStorage,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseDatabase {
	return &baseDatabase{st: st, encs: encs, enc: enc}
}

func (db *baseDatabase) Close() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close baseDatabase")
	}

	return nil
}

func (db *baseDatabase) Remove() error {
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

func (db *baseDatabase) loadHinter(b []byte) (interface{}, error) {
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

func (db *baseDatabase) loadHint(b []byte) (hint.Hint, []byte, error) {
	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return hint.Hint{}, nil, err
	}

	return ht, b[hint.MaxHintLength:], nil
}

func (db *baseDatabase) marshal(i interface{}) ([]byte, error) {
	b, err := db.enc.Marshal(i)
	if err != nil {
		return nil, err
	}

	return db.encodeWithEncoder(b), nil
}

func (db *baseDatabase) encodeWithEncoder(b []byte) []byte {
	h := make([]byte, hint.MaxHintLength)
	copy(h, db.enc.Hint().Bytes())

	return util.ConcatBytesSlice(h, b)
}

func manifestDBKey() []byte {
	return keyPrefixManifest
}

func suffrageDBKey() []byte {
	return keyPrefixSuffrage
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
