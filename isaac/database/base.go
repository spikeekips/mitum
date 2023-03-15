package isaacdatabase

import (
	"bytes"
	"io"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
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

func (db *baseDatabase) marshal(i interface{}, meta util.Byter) ([]byte, []byte, error) {
	w := bytes.NewBuffer(nil)
	defer w.Reset()

	if err := db.writeHeader(w, meta); err != nil {
		return nil, nil, err
	}

	b, err := db.enc.Marshal(i)
	if err != nil {
		return nil, nil, err
	}

	if _, err := w.Write(b); err != nil {
		return nil, nil, errors.WithStack(err)
	}

	return w.Bytes(), b, nil
}

func (db *baseDatabase) readEncoder(b []byte) (enc encoder.Encoder, meta, body []byte, err error) {
	var ht hint.Hint

	ht, meta, body, err = db.readHeader(b)
	if err != nil {
		return nil, nil, nil, err
	}

	switch enc = db.encs.Find(ht); {
	case enc == nil:
		return nil, nil, nil, util.ErrNotFound.Errorf("encoder not found for %q", ht)
	default:
		return enc, meta, body, nil
	}
}

func (db *baseDatabase) readHinter(b []byte, v interface{}) error {
	switch enc, _, body, err := db.readEncoder(b); {
	case err != nil:
		return err
	default:
		return encoder.Decode(enc, body, v)
	}
}

func (db *baseDatabase) readHinterWithEncoder(enchint hint.Hint, b []byte, v interface{}) error {
	enc := db.encs.Find(enchint)
	if enc == nil {
		return util.ErrNotFound.Errorf("encoder not found for %q", enchint)
	}

	return encoder.Decode(enc, b, v)
}

func (db *baseDatabase) writeHeader(w io.Writer, meta util.Byter) error {
	if err := util.LengthedBytes(w, db.enc.Hint().Bytes()); err != nil {
		return err
	}

	var metab []byte
	if meta != nil {
		metab = meta.Bytes()
	}

	return util.LengthedBytes(w, metab)
}

func (*baseDatabase) readHeader(b []byte) (ht hint.Hint, meta, body []byte, err error) {
	e := util.StringErrorFunc("read hint")

	htb, left, err := util.ReadLengthedBytes(b)
	if err != nil {
		return ht, nil, nil, e(err, "")
	}

	ht, err = hint.ParseHint(string(htb))
	if err != nil {
		return ht, nil, nil, e(err, "")
	}

	meta, left, err = util.ReadLengthedBytes(left)
	if err != nil {
		return ht, nil, nil, e(err, "")
	}

	return ht, meta, left, nil
}

func (db *baseDatabase) decodeSuffrage(b []byte) (base.State, error) {
	e := util.StringErrorFunc("load suffrage")

	var st base.State

	if err := db.readHinter(b, &st); err != nil {
		return nil, e(err, "load suffrage state")
	}

	if !base.IsSuffrageNodesState(st) {
		return nil, errors.Errorf("not suffrage state")
	}

	return st, nil
}

func (db *baseDatabase) getRecord(
	key []byte,
	f func(key []byte) ([]byte, bool, error),
	v interface{},
) (bool, error) {
	var body []byte

	switch b, found, err := f(key); {
	case err != nil:
		return false, err
	case !found:
		return false, nil
	default:
		body = b
	}

	enchint, _, b, err := db.readHeader(body)
	if err != nil {
		return true, err
	}

	if err := db.readHinterWithEncoder(enchint, b, v); err != nil {
		return true, err
	}

	return true, nil
}

func (db *baseDatabase) getRecordBytes(
	key []byte,
	f func(key []byte) ([]byte, bool, error),
) (enchint hint.Hint, meta, body []byte, found bool, err error) {
	switch b, found, err := f(key); {
	case err != nil:
		return enchint, nil, nil, false, err
	case !found:
		return enchint, nil, nil, false, nil
	default:
		enchint, meta, b, err = db.readHeader(b)

		return enchint, meta, b, found, err
	}
}

func NewHashRecordMeta(h util.Hash) util.Byter {
	var hb []byte
	if h != nil {
		hb = h.Bytes()
	}

	b, _ := util.NewLengthedBytesSlice(0x01, [][]byte{hb}) //nolint:gomnd //...

	return util.BytesToByter(b)
}

func ReadHashRecordMeta(b []byte) (util.Hash, error) {
	e := util.StringErrorFunc("read state record meta")

	switch _, m, _, err := util.ReadLengthedBytesSlice(b); {
	case err != nil:
		return nil, e(err, "")
	case len(m) < 1:
		return nil, e(nil, "empty state hash")
	default:
		h := valuehash.NewBytes(m[0])
		if err := h.IsValid(nil); err != nil {
			return nil, e(err, "")
		}

		return h, nil
	}
}
