package isaacdatabase

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/valuehash"
)

func writeFrame(headers [][]byte, b []byte) ([]byte, error) {
	fw, buf := util.NewBufferBytesFrameWriter()
	defer buf.Reset()

	if err := fw.Header(headers...); err != nil {
		return nil, err
	}

	if _, err := fw.Writer().Write(b); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf.Bytes(), nil
}

func ReadFrame(b []byte) (enchint string, headers [][]byte, body []byte, _ error) {
	fr, buf, err := util.NewBufferBytesFrameReader(b)
	if err != nil {
		return "", nil, nil, err
	}

	defer buf.Reset()

	switch hs, err := fr.Header(); {
	case err != nil:
		return "", nil, nil, err
	case len(hs) < 1:
	default:
		enchint = string(hs[0])

		if len(hs) > 1 {
			headers = hs[1:]
		}
	}

	switch i, err := fr.Body(); {
	case err != nil:
		return "", nil, nil, err
	default:
		return enchint, headers, i, nil
	}
}

func EncodeFrame(enc encoder.Encoder, headers [][]byte, v interface{}) (marshaled, body []byte, _ error) {
	marshaled, err := enc.Marshal(v)
	if err != nil {
		return nil, nil, err
	}

	var nb [][]byte

	switch {
	case len(headers) > 0:
		nb = make([][]byte, len(headers)+1)
		nb[0] = enc.Hint().Bytes()
		copy(nb[1:], headers)
	default:
		nb = [][]byte{enc.Hint().Bytes()}
	}

	b, err := writeFrame(nb, marshaled)
	if err != nil {
		return nil, nil, err
	}

	return marshaled, b, nil
}

func DecodeFrame[T any](encs *encoder.Encoders, enchint string, b []byte, v *T) error {
	switch _, enc, found, err := encs.FindByString(enchint); {
	case err != nil:
		return err
	case !found:
		return util.ErrNotFound.Errorf("encoder not found for %q", enchint)
	default:
		return encoder.Decode(enc, b, v)
	}
}

func ReadDecodeFrame[T any](encs *encoder.Encoders, b []byte, v *T) error {
	enchint, _, body, err := ReadFrame(b)
	if err != nil {
		return err
	}

	return DecodeFrame(encs, enchint, body, v)
}

func EncodeNoHeadersFrame(enc encoder.Encoder, v interface{}) ([]byte, error) {
	fw, buf := util.NewBufferBytesFrameWriter()
	defer buf.Reset()

	b, err := enc.Marshal(v)
	if err != nil {
		return nil, err
	}

	if _, err := fw.Writer().Write(b); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf.Bytes(), nil
}

func ReadNoHeadersFrame(b []byte) ([]byte, error) {
	fr, buf, err := util.NewBufferBytesNoHeadersFrameReader(b)
	if err != nil {
		return nil, err
	}

	defer buf.Reset()

	switch i, err := fr.Body(); {
	case err != nil:
		return nil, err
	default:
		return i, nil
	}
}

func ReadDecodeOneHeaderFrame[T any](encs *encoder.Encoders, b []byte, v *T) (header []byte, _ error) {
	enchint, header, body, err := ReadOneHeaderFrame(b)
	if err != nil {
		return nil, err
	}

	if err := DecodeFrame(encs, enchint, body, v); err != nil {
		return nil, err
	}

	return header, nil
}

func EncodeOneHeaderFrame(enc encoder.Encoder, header []byte, v interface{}) (marshaled, body []byte, _ error) {
	switch marshaled, b, err := EncodeFrame(enc, [][]byte{header}, v); {
	case err != nil:
		return nil, nil, err
	default:
		return marshaled, b, nil
	}
}

func ReadOneHeaderFrame(b []byte) (enchint string, header, body []byte, _ error) {
	enchint, headers, body, err := ReadFrame(b)
	if err != nil {
		return enchint, nil, nil, err
	}

	if len(headers) < 1 {
		return enchint, nil, nil, errors.Errorf("empty header")
	}

	return enchint, headers[0], body, nil
}

func WriteFrameHeaderOperation(op base.Operation) ([]byte, error) {
	fw, buf := util.NewBufferBytesFrameWriter()
	defer buf.Reset()

	var htb []byte
	if i, ok := op.Fact().(hint.Hinter); ok {
		htb = i.Hint().Bytes()
	}

	err := fw.Header(
		[]byte{0x00, 0x01}, // version
		util.Int64ToBytes(localtime.Now().UTC().UnixNano()), // NOTE added UTC timestamp(10)
		htb,
		op.Hash().Bytes(),
		op.Fact().Hash().Bytes(),
	)

	return buf.Bytes(), err
}

func ReadFrameHeaderOperation(b []byte) (header FrameHeaderPoolOperation, _ error) {
	fr, buf, err := util.NewBufferBytesFrameReader(b)
	if err != nil {
		return header, err
	}

	defer buf.Reset()

	var headers [][]byte

	switch hs, herr := fr.Header(); {
	case herr != nil:
		return header, herr
	case len(hs) != 5: //nolint:mnd //...
		return header, errors.Errorf("wrong size operation header")
	default:
		headers = hs
	}

	header.version = [2]byte{headers[0][0], headers[0][1]}

	nsec, err := util.BytesToInt64(headers[1])
	if err != nil {
		return header, errors.WithMessage(err, "wrong added at time")
	}

	header.addedAt = time.Unix(0, nsec)

	header.ht, err = hint.ParseHint(string(headers[2]))
	if err != nil {
		return header, errors.Errorf("wrong hint")
	}

	if err := header.ht.IsValid(nil); err != nil {
		return header, errors.WithMessage(err, "wrong hint")
	}

	header.ophash = valuehash.Bytes(headers[3])
	header.facthash = valuehash.Bytes(headers[4])

	return header, nil
}

type FrameHeaderPoolOperation struct {
	addedAt  time.Time
	ophash   util.Hash
	facthash util.Hash
	ht       hint.Hint
	version  [2]byte
}

func (h FrameHeaderPoolOperation) Version() [2]byte {
	return h.version
}

func (h FrameHeaderPoolOperation) AddedAt() time.Time {
	return h.addedAt
}

func (h FrameHeaderPoolOperation) Hint() hint.Hint {
	return h.ht
}

func (h FrameHeaderPoolOperation) Operation() util.Hash {
	return h.ophash
}

func (h FrameHeaderPoolOperation) Fact() util.Hash {
	return h.facthash
}

func EncodeFrameSuffrageExpelOperation(enc encoder.Encoder, op base.SuffrageExpelOperation) ([]byte, error) {
	fw, buf := util.NewBufferBytesFrameWriter()
	defer buf.Reset()

	b, err := enc.Marshal(op)
	if err != nil {
		return nil, err
	}

	fact := op.ExpelFact()

	if err := fw.Header(
		enc.Hint().Bytes(),
		[]byte{0x00, 0x01},
		fact.Node().Bytes(),
		util.Int64ToBytes(fact.ExpelStart().Int64()),
		util.Int64ToBytes(fact.ExpelEnd().Int64()),
	); err != nil {
		return nil, err
	}

	if _, err := fw.Writer().Write(b); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf.Bytes(), nil
}

func ReadFrameHeaderSuffrageExpelOperation(b []byte) (
	enchint string,
	r FrameHeaderSuffrageExpelOperation,
	left []byte,
	_ error,
) {
	switch enchint, headers, body, err := ReadFrame(b); {
	case err != nil:
		return enchint, r, nil, err
	default:
		i, err := readFrameHeaderSuffrageExpelOperation(headers)
		if err != nil {
			return enchint, r, nil, err
		}

		return enchint, i, body, nil
	}
}

func readFrameHeaderSuffrageExpelOperation(headers [][]byte) (
	r FrameHeaderSuffrageExpelOperation,
	_ error,
) {
	switch {
	case len(headers) < 4: //nolint:mnd //...
		return r, errors.Errorf("missing header")
	case len(headers[1]) < 1:
		return r, errors.Errorf("wrong format; empty node")
	default:
		s, err := util.BytesToInt64(headers[2])
		if err != nil {
			return r, errors.WithMessage(err, "wrong start height")
		}

		e, err := util.BytesToInt64(headers[3])
		if err != nil {
			return r, errors.WithMessage(err, "wrong end height")
		}

		return FrameHeaderSuffrageExpelOperation{
			node:  headers[1],
			start: s,
			end:   e,
		}, nil
	}
}

type FrameHeaderSuffrageExpelOperation struct {
	node  []byte
	start int64
	end   int64
}

func (r FrameHeaderSuffrageExpelOperation) Node() []byte {
	return r.node
}

func (r FrameHeaderSuffrageExpelOperation) Start() int64 {
	return r.start
}

func (r FrameHeaderSuffrageExpelOperation) End() int64 {
	return r.end
}

func EncodeFrameState(enc encoder.Encoder, st base.State) ([]byte, error) {
	_, b, err := EncodeOneHeaderFrame(enc, st.Hash().Bytes(), st)

	return b, err
}
