package launch

import (
	"context"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	isaacstates "github.com/spikeekips/mitum/isaac/states"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/ps"
)

var PNameStatesNetworkHandlers = ps.Name("states-network-handlers")

func PStatesNetworkHandlers(pctx context.Context) (context.Context, error) {
	var encs *encoder.Encoders
	var enc encoder.Encoder
	var local base.LocalNode
	var params *isaac.LocalParams
	var handlers *quicstream.PrefixHandler
	var states *isaacstates.States
	var pool *isaacdatabase.TempPool
	var proposalMaker *isaac.ProposalMaker
	var db isaac.Database
	var client *isaacnetwork.QuicstreamClient

	if err := util.LoadFromContext(pctx,
		EncodersContextKey, &encs,
		EncoderContextKey, &enc,
		LocalContextKey, &local,
		LocalParamsContextKey, &params,
		QuicstreamHandlersContextKey, &handlers,
		StatesContextKey, &states,
		PoolDatabaseContextKey, &pool,
		ProposalMakerContextKey, &proposalMaker,
		CenterDatabaseContextKey, &db,
		QuicstreamClientContextKey, &client,
	); err != nil {
		return pctx, err
	}

	handlers.
		Add(isaacnetwork.HandlerPrefixSetAllowConsensus,
			quicstreamheader.NewHandler(encs,
				time.Second*2, //nolint:gomnd //...
				isaacnetwork.QuicstreamHandlerSetAllowConsensus(
					local.Publickey(),
					params.NetworkID(),
					states.SetAllowConsensus,
				),
				nil,
			),
		).
		Add(isaacnetwork.HandlerPrefixRequestProposal,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerRequestProposal(
				local, pool, proposalMaker, db.LastBlockMap,
				func(ctx context.Context, header isaacnetwork.RequestProposalRequestHeader) (
					base.ProposalSignFact, error,
				) {
					var connInfo quicstream.UDPConnInfo

					switch broker := states.HandoverXBroker(); {
					case broker == nil:
						return nil, nil
					default:
						connInfo = broker.ConnInfo()
					}

					switch pr, _, err := client.RequestProposal(ctx, connInfo,
						header.Point(),
						header.Proposer(),
						header.PreviousBlock(),
					); {
					case err != nil:
						return nil, err
					default:
						return pr, nil
					}
				},
			), nil)).
		Add(isaacnetwork.HandlerPrefixProposal,
			quicstreamheader.NewHandler(encs, 0, isaacnetwork.QuicstreamHandlerProposal(
				pool,
				func(ctx context.Context, header isaacnetwork.ProposalRequestHeader) (hint.Hint, []byte, bool, error) {
					var connInfo quicstream.UDPConnInfo

					switch broker := states.HandoverXBroker(); {
					case broker == nil:
						return hint.Hint{}, nil, false, nil
					default:
						connInfo = broker.ConnInfo()
					}

					switch pr, _, err := client.Proposal(ctx, connInfo, header.Proposal()); {
					case err != nil:
						return hint.Hint{}, nil, false, err
					case pr == nil:
						return hint.Hint{}, nil, false, nil
					default:
						b, err := enc.Marshal(pr)
						if err != nil {
							return hint.Hint{}, nil, false, err
						}

						return enc.Hint(), b, true, nil
					}
				},
			), nil))

	return pctx, nil
}
