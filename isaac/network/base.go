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
	HandlerPrefixRequestProposal        = "request_proposal"
	HandlerPrefixProposal               = "proposal"
	HandlerPrefixLastSuffrageProof      = "last_suffrage_proof"
	HandlerPrefixSuffrageProof          = "suffrage_proof"
	HandlerPrefixLastBlockMap           = "last_blockmap"
	HandlerPrefixBlockMap               = "blockmap"
	HandlerPrefixBlockMapItem           = "blockmap_item"
	HandlerPrefixMemberlist             = "memberlist"
	HandlerPrefixNodeChallenge          = "node_challenge"
	HandlerPrefixSuffrageNodeConnInfo   = "suffrage_node_conninfo"
	HandlerPrefixSyncSourceConnInfo     = "sync_source_conninfo"
	HandlerPrefixOperation              = "operation"
	HandlerPrefixSendOperation          = "send_operation"
	HandlerPrefixState                  = "state"
	HandlerPrefixExistsInStateOperation = "exists_instate_operation"
	HandlerPrefixNodeInfo               = "node_info"
	HandlerPrefixCallbackMessage        = "callback_message"
	HandlerPrefixSendBallots            = "send_ballots"
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

func WriteResponse(w io.Writer, header isaac.NetworkHeader, body interface{}, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to write response")

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

	if body != nil {
		if err := enc.StreamEncoder(w).Encode(body); err != nil {
			return e(err, "failed to write body")
		}
	}

	return nil
}

func WriteResponseBytes(w io.Writer, header isaac.NetworkHeader, body []byte, enc encoder.Encoder) error {
	e := util.StringErrorFunc("failed to write response")

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

	if _, err := ensureWrite(w, body); err != nil {
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
