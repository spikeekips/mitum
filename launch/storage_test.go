//go:build test && redis
// +build test,redis

package launch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type testLoadPermanentDatabase struct {
	suite.Suite
}

func (t *testLoadPermanentDatabase) TestXXX() {
	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()

	root, err := os.MkdirTemp("", "leveldb")
	t.NoError(err)
	defer func() {
		os.RemoveAll(root)
	}()

	newleveldbroot := func() string {
		return filepath.Join(root, util.UUID().String())
	}

	cases := []struct {
		name string
		uri  string
		err  string
	}{
		{name: "leveldb: ok", uri: newleveldbroot()},
		{name: "leveldb: empty", uri: "", err: "empty path"},
		{name: "leveldb: empty path", uri: "leveldb://", err: "empty path"},
		{name: "redis: ok", uri: "redis://"},
		{name: "redis: with redis", uri: "redis+redis://"},
		{name: "redis: with empty network", uri: "redis+://"},
		{name: "redis: with unknown network", uri: "redis+showme://", err: "redis: invalid URL scheme"},
		{name: "unknown database type", uri: "showme://", err: "unsupported database type"},
	}

	for i := range cases {
		i := i
		c := cases[i]
		t.Run(
			c.name,
			func() {
				perm, err := LoadPermanentDatabase(c.uri, util.UUID().String(), encs, enc)
				switch {
				case len(c.err) > 0:
					if err == nil {
						t.NoError(errors.Errorf("expected %q, but nil error", c.err), "%d: %v", i, c.name)

						return
					}

					t.ErrorContains(err, c.err, "%d: %v", i, c.name)
				case err != nil:
					t.NoError(errors.Errorf("expected nil error, but %+v", err), "%d: %v", i, c.name)

					return
				default:
					t.NotNil(perm)
				}
			},
		)
	}
}

func TestLoadPermanentDatabase(t *testing.T) {
	suite.Run(t, new(testLoadPermanentDatabase))
}
