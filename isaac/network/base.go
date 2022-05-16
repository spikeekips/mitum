package isaacnetwork

import (
	"io"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var (
	HandlerPrefixRequestProposal = "request_proposal"
	HandlerPrefixProposal        = "proposal"
	HandlerPrefixSuffrageProof   = "suffrage_proof"
	HandlerPrefixLastBlockMap    = "last_blockmap"
	HandlerPrefixBlockMap        = "blockmap"
	HandlerPrefixBlockMapItem    = "blockmap_item"
)

type baseNetwork struct {
	encs *encoder.Encoders
	enc  encoder.Encoder
}

func newBaseNetwork(
	encs *encoder.Encoders,
	enc encoder.Encoder,
) *baseNetwork {
	return &baseNetwork{
		encs: encs,
		enc:  enc,
	}
}

func (c *baseNetwork) response(w io.Writer, header Header, body interface{}, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to write response")

	var b []byte
	if body != nil {
		i, err := enc.Marshal(body)
		if err != nil {
			return e(err, "")
		}

		b = i
	}

	if err := writeHint(w, enc.Hint()); err != nil {
		return e(err, "")
	}

	var headerb []byte
	if header != nil {
		i, err := enc.Marshal(header)
		if err != nil {
			return e(err, "")
		}

		headerb = i
	}

	if err := writeHeader(w, headerb); err != nil {
		return e(err, "")
	}

	if _, err := ensureWrite(w, b); err != nil {
		return e(err, "failed to write body")
	}

	return nil
}

func (c *baseNetwork) readEncoder(r io.Reader) (encoder.Encoder, error) {
	e := util.StringErrorFunc("failed to read encoder")

	ht, err := readHint(r)
	if err != nil {
		return nil, e(err, "")
	}

	switch enc := c.encs.Find(ht); {
	case enc == nil:
		return nil, e(util.ErrNotFound.Errorf("encoder not found for %q", ht), "")
	default:
		return enc, nil
	}
}

func (c *baseNetwork) readHinter(r io.Reader, enc encoder.Encoder, v interface{}) error {
	e := util.StringErrorFunc("failed to read hinter")

	b, err := io.ReadAll(r)
	if err != nil {
		return e(err, "")
	}

	hinter, err := enc.Decode(b)
	if err != nil {
		return e(err, "")
	}

	if err := util.InterfaceSetValue(hinter, v); err != nil {
		return e(err, "")
	}

	return nil
}
