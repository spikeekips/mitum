package isaacnetwork

import (
	"context"
	"io"
	"time"

	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var (
	HandlerPrefixRequestProposal   = "request_proposal"
	HandlerPrefixProposal          = "proposal"
	HandlerPrefixLastSuffrageProof = "last_suffrage_proof"
	HandlerPrefixSuffrageProof     = "suffrage_proof"
	HandlerPrefixLastBlockMap      = "last_blockmap"
	HandlerPrefixBlockMap          = "blockmap"
	HandlerPrefixBlockMapItem      = "blockmap_item"
	HandlerPrefixMemberlist        = "memberlist"
	HandlerPrefixNodeChallenge     = "node_challenge"
)

type baseNetwork struct {
	encs        *encoder.Encoders
	enc         encoder.Encoder
	idleTimeout time.Duration
}

func newBaseNetwork(
	encs *encoder.Encoders,
	enc encoder.Encoder,
	idleTimeout time.Duration,
) *baseNetwork {
	return &baseNetwork{
		encs:        encs,
		enc:         enc,
		idleTimeout: idleTimeout,
	}
}

func Response(w io.Writer, header isaac.NetworkHeader, body interface{}, enc encoder.Encoder) error {
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

func ReadEncoder(ctx context.Context, encs *encoder.Encoders, r io.Reader) (encoder.Encoder, error) {
	e := util.StringErrorFunc("failed to read encoder")

	ht, err := readHint(ctx, r)
	if err != nil {
		return nil, e(err, "")
	}

	switch enc := encs.Find(ht); {
	case enc == nil:
		return nil, e(util.ErrNotFound.Errorf("encoder not found for %q", ht), "")
	default:
		return enc, nil
	}
}
