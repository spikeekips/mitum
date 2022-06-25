package base

import (
	"bytes"
	"encoding/base64"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

// NetworkID will be used to separate mitum network with the other mitum
// network. Generally it is used for hashing and making signature.
type NetworkID []byte

const MaxNetworkIDLength = 300

func (ni NetworkID) IsValid([]byte) error {
	switch {
	case len(ni) < 1:
		return util.ErrInvalid.Errorf("empty network id")
	case len(ni) > MaxNetworkIDLength:
		return util.ErrInvalid.Errorf(
			"network id too long; %d < max=%d",
			len(ni),
			MaxNetworkIDLength,
		)
	default:
		return nil
	}
}

func (ni NetworkID) Equal(a NetworkID) bool {
	if a == nil {
		return false
	}

	return bytes.Equal([]byte(ni), []byte(a))
}

func (ni NetworkID) MarshalText() ([]byte, error) {
	return []byte(base64.StdEncoding.EncodeToString(ni.Bytes())), nil
}

func (ni *NetworkID) UnmarshalText(b []byte) error {
	s, err := base64.StdEncoding.DecodeString(string(b))
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal NetworkID")
	}

	*ni = NetworkID(s)

	return nil
}

func (ni NetworkID) Bytes() []byte {
	return []byte(ni)
}
