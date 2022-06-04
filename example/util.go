package main

import (
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
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

func defaultPermanentDatabaseURI() string {
	uri, found := os.LookupEnv(envKeyPermanentDatabase)
	if !found {
		return "redis://"
	}

	return uri
}

func defaultLocalFSRoot(addr base.Address) (string, error) {
	switch root, found := os.LookupEnv(envKeyFSRoot); {
	case found:
		i, err := filepath.Abs(root)
		if err != nil {
			return "", errors.Wrap(err, "")
		}

		return i, nil
	default:
		return filepath.Join(os.TempDir(), "mitum-example-"+addr.String()), nil
	}
}
