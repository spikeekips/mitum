package main

import (
	"time"

	"github.com/spikeekips/mitum/network/quicstream"
)

// FIXME remove file

var networkHandlerIdleTimeout = time.Second * 10

func (cmd *RunCommand) networkHandlers() *quicstream.PrefixHandler {
	// operationfilterf := cmd.newOperationFilter()

	/*
		handlers := isaacnetwork.NewQuicstreamHandlers(
			cmd.local,
			cmd.nodePolicy,
			cmd.encs,
			cmd.enc,
			networkHandlerIdleTimeout,
			cmd.pool,
			cmd.pool,
			cmd.proposalMaker,
			func(last util.Hash) (base.SuffrageProof, bool, error) {
				switch proof, found, err := cmd.db.LastSuffrageProof(); {
				case err != nil:
					return nil, false, err
				case !found:
					return nil, false, storage.NotFoundError.Errorf("last SuffrageProof not found")
				case last != nil && last.Equal(proof.Map().Manifest().Suffrage()):
					return nil, false, nil
				default:
					return proof, true, nil
				}
			},
			cmd.db.SuffrageProof,
			func(last util.Hash) (base.BlockMap, bool, error) {
				switch m, found, err := cmd.db.LastBlockMap(); {
				case err != nil:
					return nil, false, err
				case !found:
					return nil, false, storage.NotFoundError.Errorf("last BlockMap not found")
				case last != nil && last.Equal(m.Manifest().Hash()):
					return nil, false, nil
				default:
					return m, true, nil
				}
			},
			cmd.db.BlockMap,
			func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
				e := util.StringErrorFunc("failed to get BlockMapItem")

				var enc encoder.Encoder

				switch m, found, err := cmd.db.BlockMap(height); {
				case err != nil:
					return nil, false, e(err, "")
				case !found:
					return nil, false, e(storage.NotFoundError.Errorf("BlockMap not found"), "")
				default:
					enc = cmd.encs.Find(m.Encoder())
				}

				reader, err := isaacblock.NewLocalFSReaderFromHeight(
					launch.LocalFSDataDirectory(cmd.design.Storage.Base), height, enc,
				)
				if err != nil {
					return nil, false, e(err, "")
				}
				defer func() {
					_ = reader.Close()
				}()

				return reader.Reader(item)
			},
			func() ([]isaac.NodeConnInfo, error) {
				// FIXME cache result

				var suf base.Suffrage

				switch proof, found, err := cmd.db.LastSuffrageProof(); {
				case err != nil:
					return nil, err
				case !found:
					return nil, storage.NotFoundError.Errorf("last SuffrageProof not found")
				default:
					i, err := proof.Suffrage()
					if err != nil {
						return nil, err
					}

					suf = i
				}

				members := make([]isaac.NodeConnInfo, cmd.memberlist.MembersLen()*2)

				var i int
				cmd.memberlist.Remotes(func(node quicmemberlist.Node) bool {
					if !suf.ExistsPublickey(node.Address(), node.Publickey()) {
						return true
					}

					members[i] = isaacnetwork.NewNodeConnInfoFromMemberlistNode(node)
					i++

					return true
				})

				return members[:i], nil
			},
			func() ([]isaac.NodeConnInfo, error) {
				// FIXME cache result

				members := make([]isaac.NodeConnInfo, cmd.syncSourcePool.Len()*2)

				var i int
				cmd.syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
					members[i] = nci
					i++

					return true
				})

				return members[:i], nil
			},
			cmd.db.State,
			cmd.db.ExistsInStateOperation,
			operationfilterf,
			cmd.nodeinfo,
		)
	*/

	//design := cmd.design
	//encs := cmd.encs
	//enc := cmd.enc
	//idletimeout := networkHandlerIdleTimeout
	//local := cmd.local
	//policy := cmd.nodePolicy
	//db := cmd.db
	//pool := cmd.pool
	//proposalMaker := cmd.proposalMaker
	//memberlist := cmd.memberlist
	//syncSourcePool := cmd.syncSourcePool
	//nodeinfo := cmd.nodeinfo // FIXME in sync state, manifest missing
	//
	//lastBlockMapf := quicstreamHandlerLastBlockMapFunc(db)
	//suffrageNodeConnInfof := quicstreamHandlerSuffrageNodeConnInfoFunc(db, memberlist)

	//cmd.handlers.
	//Add(isaacnetwork.HandlerPrefixOperation,
	//	isaacnetwork.QuicstreamHandlerOperation(encs, idletimeout, pool),
	//).
	//Add(isaacnetwork.HandlerPrefixSendOperation,
	//	isaacnetwork.QuicstreamHandlerSendOperation(encs, idletimeout, policy, pool, db.ExistsInStateOperation, operationfilterf),
	//).
	//Add(isaacnetwork.HandlerPrefixRequestProposal,
	//	isaacnetwork.QuicstreamHandlerRequestProposal(encs, idletimeout,
	//		local, pool, proposalMaker, lastBlockMapf,
	//	),
	//).
	//Add(isaacnetwork.HandlerPrefixProposal,
	//	isaacnetwork.QuicstreamHandlerProposal(encs, idletimeout, pool),
	//).
	//Add(isaacnetwork.HandlerPrefixLastSuffrageProof,
	//	isaacnetwork.QuicstreamHandlerLastSuffrageProof(encs, idletimeout,
	//		func(last util.Hash) (base.SuffrageProof, bool, error) {
	//			switch proof, found, err := db.LastSuffrageProof(); {
	//			case err != nil:
	//				return nil, false, err
	//			case !found:
	//				return nil, false, storage.NotFoundError.Errorf("last SuffrageProof not found")
	//			case last != nil && last.Equal(proof.Map().Manifest().Suffrage()):
	//				return nil, false, nil
	//			default:
	//				return proof, true, nil
	//			}
	//		},
	//	),
	//).
	//Add(isaacnetwork.HandlerPrefixSuffrageProof,
	//	isaacnetwork.QuicstreamHandlerSuffrageProof(encs, idletimeout, db.SuffrageProof),
	//).
	//Add(isaacnetwork.HandlerPrefixLastBlockMap,
	//	isaacnetwork.QuicstreamHandlerLastBlockMap(encs, idletimeout, lastBlockMapf),
	//).
	//Add(isaacnetwork.HandlerPrefixBlockMap,
	//	isaacnetwork.QuicstreamHandlerBlockMap(encs, idletimeout, db.BlockMap),
	//).
	//Add(isaacnetwork.HandlerPrefixBlockMapItem,
	//	isaacnetwork.QuicstreamHandlerBlockMapItem(encs, idletimeout, idletimeout*2,
	//		func(height base.Height, item base.BlockMapItemType) (io.ReadCloser, bool, error) {
	//			e := util.StringErrorFunc("failed to get BlockMapItem")

	//			var enc encoder.Encoder

	//			switch m, found, err := db.BlockMap(height); {
	//			case err != nil:
	//				return nil, false, e(err, "")
	//			case !found:
	//				return nil, false, e(storage.NotFoundError.Errorf("BlockMap not found"), "")
	//			default:
	//				enc = encs.Find(m.Encoder())
	//			}

	//			reader, err := isaacblock.NewLocalFSReaderFromHeight(
	//				launch.LocalFSDataDirectory(design.Storage.Base), height, enc,
	//			)
	//			if err != nil {
	//				return nil, false, e(err, "")
	//			}
	//			defer func() {
	//				_ = reader.Close()
	//			}()

	//			return reader.Reader(item)
	//		},
	//	),
	//).
	//Add(isaacnetwork.HandlerPrefixNodeChallenge,
	//	isaacnetwork.QuicstreamHandlerNodeChallenge(encs, idletimeout, local, policy),
	//).
	//Add(isaacnetwork.HandlerPrefixSuffrageNodeConnInfo,
	//	isaacnetwork.QuicstreamHandlerSuffrageNodeConnInfo(encs, idletimeout, suffrageNodeConnInfof),
	//).
	//Add(isaacnetwork.HandlerPrefixSyncSourceConnInfo,
	//	isaacnetwork.QuicstreamHandlerSyncSourceConnInfo(encs, idletimeout,
	//		func() ([]isaac.NodeConnInfo, error) {
	//			// FIXME cache result

	//			members := make([]isaac.NodeConnInfo, syncSourcePool.Len()*2)

	//			var i int
	//			syncSourcePool.Actives(func(nci isaac.NodeConnInfo) bool {
	//				members[i] = nci
	//				i++

	//				return true
	//			})

	//			return members[:i], nil
	//		},
	//	),
	//).
	//Add(isaacnetwork.HandlerPrefixState,
	//	isaacnetwork.QuicstreamHandlerState(encs, idletimeout, db.State),
	//).
	//Add(isaacnetwork.HandlerPrefixExistsInStateOperation,
	//	isaacnetwork.QuicstreamHandlerExistsInStateOperation(encs, idletimeout, db.ExistsInStateOperation),
	//).
	//Add(isaacnetwork.HandlerPrefixNodeInfo,
	//	isaacnetwork.QuicstreamHandlerNodeInfo(encs, idletimeout, quicstreamHandlerGetNodeInfoFunc(enc, nodeinfo)),
	//)
	// Add(launch.HandlerPrefixPprof, launch.NetworkHandlerPprofFunc(encs))

	return cmd.handlers
}
