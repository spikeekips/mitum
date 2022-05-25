package main

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/launch"
	"github.com/spikeekips/mitum/util/encoder"
)

type newProposalProcessorFunc func(proposal base.ProposalSignedFact, previous base.Manifest) (
	isaac.ProposalProcessor, error)

func prepareLocal(address base.Address) (base.LocalNode, error) {
	// NOTE make privatekey, based on node address
	b := make([]byte, base.PrivatekeyMinSeedSize)
	copy(b, address.Bytes())

	priv, err := base.NewMPrivatekeyFromSeed(string(b))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create privatekey from node address")
	}

	log.Info().
		Stringer("address", address).
		Stringer("privatekey", priv).
		Stringer("publickey", priv.Publickey()).
		Msg("keypair generated")

	return isaac.NewLocalNode(priv, address), nil
}

func loadPermanentDatabase(_ string, encs *encoder.Encoders, enc encoder.Encoder) (isaac.PermanentDatabase, error) {
	// uri := launch.DBRootPermDirectory(dbroot)
	uri := "redis://"

	return launch.LoadPermanentDatabase(uri, encs, enc)
}

func defaultDBRoot(addr base.Address) string {
	dbroot, found := os.LookupEnv(envKeyFSRootf)
	if !found {
		dbroot = filepath.Join(os.TempDir(), "mitum-example-"+addr.String())
	}

	return dbroot
}
