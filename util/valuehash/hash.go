package valuehash

import (
	"fmt"

	"github.com/spikeekips/mitum/util"
)

type Hash interface {
	util.IsValider
	util.Byter
	// NOTE usually String() value is the base58 encoded of Bytes()
	fmt.Stringer
	Equal(Hash) bool
}

type Hasher interface {
	Hash() Hash
}

type HashGenerator interface {
	GenerateHash() Hash
}
