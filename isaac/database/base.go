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
		return nil, errors.Wrap(err, "")
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
		return nil, nil, util.ErrNotFound.Errorf("encoder not found for %q", ht)
	default:
		return enc, raw, nil
	}
}

func (db *baseDatabase) readHinter(b []byte, v interface{}) error {
	if b == nil {
		return nil
	}

	switch enc, raw, err := db.readEncoder(b); {
	case err != nil:
		return errors.Wrap(err, "")
	default:
		if err := encoder.Decode(enc, raw, v); err != nil {
			return errors.Wrap(err, "")
		}

		return nil
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

func (db *baseDatabase) decodeSuffrage(b []byte) (base.State, error) {
	e := util.StringErrorFunc("failed to load suffrage")

	var st base.State

	if err := db.readHinter(b, &st); err != nil {
		return nil, e(err, "failed to load suffrage state")
	}

	if !base.IsSuffrageState(st) {
		return nil, errors.Errorf("not suffrage state")
	}

	return st, nil
}
