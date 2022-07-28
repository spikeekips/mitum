package main

import (
	"io"
	"time"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

var networkHandlerIdleTimeout = time.Second * 10

var (
	HandlerPrefixRequestState                  = "state"
	HandlerPrefixRequestExistsInStateOperation = "exists_instate_operation"
)

func (cmd *runCommand) networkHandlers() *quicstream.PrefixHandler {
	operationfilterf := cmd.newOperationFilter()

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

			// FIXME use cache with singleflight

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
	)

	prefix := launch.Handlers(cmd.encs, handlers)

	return prefix
}
