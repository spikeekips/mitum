package isaac

import (
	"bytes"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	keyPrefixManifest  = []byte{0x00, 0x00}
	keyPrefixSuffrage  = []byte{0x00, 0x01}
	keyPrefixState     = []byte{0x00, 0x02}
	keyPrefixOperation = []byte{0x00, 0x03}
)

func manifestKey() []byte {
	return keyPrefixManifest
}

func suffrageKey() []byte {
	return keyPrefixSuffrage
}

func stateKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(keyPrefixState, h.Bytes())
}

func operationKey(h util.Hash) []byte {
	return util.ConcatBytesSlice(keyPrefixOperation, h.Bytes())
}

func loadHint(b []byte) (hint.Hint, []byte, error) {
	ht, err := hint.ParseHint(string(bytes.TrimRight(b[:hint.MaxHintLength], "\x00")))
	if err != nil {
		return hint.Hint{}, nil, err
	}

	return ht, b[hint.MaxHintLength:], nil
}
