// +build test

package localfs

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/stretchr/testify/suite"

	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
)

type BaseTestLocalFS struct {
	suite.Suite
	root string
}

func (t *BaseTestLocalFS) SetupSuite() {
	p, err := os.MkdirTemp("", "fs-")
	if err != nil {
		panic(err)
	}

	t.root = p
}

func (t *BaseTestLocalFS) TearDownSuite() {
	if err := os.RemoveAll(t.root); err != nil {
		if l := t.T().Log; l == nil {
			fmt.Fprintf(os.Stderr, "%+v", err)
		} else {
			l(fmt.Sprintf("%+v", err))
		}
	}
}

func (t *BaseTestLocalFS) FS() *FS {
	root := filepath.Join(t.root, util.UUID().String())
	fs, err := NewFS(root, true)
	if err != nil {
		panic(err)
	}

	return fs
}

type BaseTestBlocks struct {
	BaseTestLocalFS
}

func (t *BaseTestBlocks) SetupSuite() {
	t.BaseTestLocalFS.SetupSuite()
}

func (t *BaseTestBlocks) TearDownSuite() {
	t.BaseTestLocalFS.TearDownSuite()
}

func (t *BaseTestBlocks) BlockFS(enc *jsonenc.Encoder) *storage.BlockFS {
	fs := t.FS()
	return storage.NewBlockFS(fs, enc)
}
