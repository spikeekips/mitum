package isaacdatabase

import (
	"bytes"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type baseDatabase struct {
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func newBaseDatabase(
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseDatabase {
	return &baseDatabase{
		encs: encs,
		enc:  enc,
	}
}

func (db *baseDatabase) marshal(i interface{}) ([]byte, error) {
	b, err := db.enc.Marshal(i)
	if err != nil {
		return nil, err
	}

	return db.marshalWithEncoder(b), nil
}

func (db *baseDatabase) readEncoder(b []byte) (encoder.Encoder, []byte, error) {
	var ht hint.Hint
	ht, raw, err := db.readHint(b)
	if err != nil {
		return nil, nil, err
	}

	switch enc := db.encs.Find(ht); {
	case enc == nil:
		return nil, nil, util.NotFoundError.Errorf("encoder not found for %q", ht)
	default:
		return enc, raw, nil
	}
}

func (db *baseDatabase) readHinter(b []byte) (interface{}, error) {
	if b == nil {
		return nil, nil
	}

	switch enc, raw, err := db.readEncoder(b); {
	case err != nil:
		return nil, errors.Wrap(err, "")
	default:
		return enc.Decode(raw)
	}
}

func (*baseDatabase) readHint(b []byte) (hint.Hint, []byte, error) {
	if len(b) < hint.MaxHintLength {
		return hint.Hint{}, nil, errors.Errorf("none hinted string; too short")
	}

	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return hint.Hint{}, nil, err
	}

	return ht, b[hint.MaxHintLength:], nil
}

func (db *baseDatabase) marshalWithEncoder(b []byte) []byte {
	h := make([]byte, hint.MaxHintLength)
	copy(h, db.enc.Hint().Bytes())

	return util.ConcatBytesSlice(h, b)
}

func (db *baseDatabase) decodeState(b []byte) (base.State, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load state")

	hinter, err := db.readHinter(b)
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

func (db *baseDatabase) decodeSuffrage(b []byte) (base.State, error) {
	e := util.StringErrorFunc("failed to load suffrage")

	switch i, err := db.decodeState(b); {
	case err != nil:
		return nil, e(err, "failed to load suffrage state")
	default:
		st, err := base.InterfaceIsSuffrageState(i)
		if err != nil {
			return nil, e(err, "")
		}

		return st, nil
	}
}

func (db *baseDatabase) decodeBlockMap(b []byte) (base.BlockMap, error) {
	if b == nil {
		return nil, nil
	}

	e := util.StringErrorFunc("failed to load blockmap")

	hinter, err := db.readHinter(b)
	switch {
	case err != nil:
		return nil, e(err, "")
	case hinter == nil:
		return nil, e(nil, "empty blockmap")
	}

	switch i, ok := hinter.(base.BlockMap); {
	case !ok:
		return nil, e(nil, "not blockmap: %T", hinter)
	default:
		return i, nil
	}
}
