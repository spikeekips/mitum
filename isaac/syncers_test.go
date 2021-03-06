package isaac

import (
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/network"
)

type testSyncers struct {
	BaseTest
}

func (t *testSyncers) TestNew() {
	ls := t.Locals(2)
	local, remote := ls[0], ls[1]

	t.SetupNodes(local, []*Local{remote})

	fromHeight := t.LastManifest(local.Storage()).Height() + 1
	target := fromHeight + 2
	t.GenerateBlocks([]*Local{remote}, target)

	baseManifest, found, err := local.Storage().LastManifest()
	t.NoError(err)
	t.True(found)

	finishedChan := make(chan base.Height)
	blocksChan := make(chan []block.Block)

	ss := NewSyncers(local.Node(), local.Storage(), local.BlockFS(), local.Policy(), baseManifest)
	ss.WhenFinished(func(height base.Height) {
		finishedChan <- height
	})
	ss.WhenBlockSaved(func(blocks []block.Block) {
		blocksChan <- blocks
	})
	t.NoError(ss.Start())

	defer ss.Stop()

	t.NoError(ss.Add(target, []network.Node{remote.Node()}))

	var blocks []base.Height
	select {
	case <-time.After(time.Second * 3):
		t.NoError(xerrors.Errorf("timeout to wait to be finished"))
	case bs := <-blocksChan:
		for _, blk := range bs {
			blocks = append(blocks, blk.Height())
		}
	case height := <-finishedChan:
		t.Equal(target, height)
		break
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i] < blocks[j]
	})

	var expectedBlocks []base.Height
	for i := fromHeight; i <= target; i++ {
		expectedBlocks = append(expectedBlocks, i)
	}

	t.Equal(expectedBlocks, blocks)

	rm, found, err := remote.Storage().LastManifest()
	t.NoError(err)
	t.True(found)
	lm, found, err := local.Storage().LastManifest()
	t.NoError(err)
	t.True(found)

	t.CompareManifest(rm, lm)
}

func (t *testSyncers) TestMultipleSyncers() {
	ls := t.Locals(2)
	local, remote := ls[0], ls[1]

	t.SetupNodes(local, []*Local{remote})

	target := t.LastManifest(local.Storage()).Height() + 2
	t.GenerateBlocks([]*Local{remote}, target)

	baseManifest, found, err := local.Storage().LastManifest()
	t.NoError(err)
	t.True(found)

	finishedChan := make(chan base.Height)

	ss := NewSyncers(local.Node(), local.Storage(), local.BlockFS(), local.Policy(), baseManifest)
	ss.WhenFinished(func(height base.Height) {
		finishedChan <- height
	})
	t.NoError(ss.Start())

	defer ss.Stop()

	for i := baseManifest.Height().Int64() + 1; i <= target.Int64(); i++ {
		t.NoError(ss.Add(base.Height(i), []network.Node{remote.Node()}))
	}

	select {
	case <-time.After(time.Second * 5):
		t.NoError(xerrors.Errorf("timeout to wait to be finished"))
	case height := <-finishedChan:
		t.Equal(target, height)
		break
	}
}

func (t *testSyncers) TestMangledFinishedOrder() {
	ls := t.Locals(2)
	local, remote := ls[0], ls[1]

	t.SetupNodes(local, []*Local{remote})

	target := t.LastManifest(local.Storage()).Height() + 10
	t.GenerateBlocks([]*Local{remote}, target)

	baseManifest, found, err := local.Storage().LastManifest()
	t.NoError(err)
	t.True(found)

	finishedChan := make(chan base.Height)

	ss := NewSyncers(local.Node(), local.Storage(), local.BlockFS(), local.Policy(), baseManifest)

	ss.WhenFinished(func(height base.Height) {
		finishedChan <- height
	})
	t.NoError(ss.Start())

	defer ss.Stop()

	t.NoError(ss.Add(target-1, []network.Node{remote.Node()}))
	t.NoError(ss.Add(target, []network.Node{remote.Node()}))

	select {
	case <-time.After(time.Second * 5):
		t.NoError(xerrors.Errorf("timeout to wait to be finished"))
	case height := <-finishedChan:
		t.Equal(target, height)
		break
	}
}

func (t *testSyncers) TestAddAfterFinished() {
	ls := t.Locals(2)
	local, remote := ls[0], ls[1]

	t.SetupNodes(local, []*Local{remote})

	target := t.LastManifest(local.Storage()).Height() + 10
	t.GenerateBlocks([]*Local{remote}, target)

	baseManifest, found, err := local.Storage().LastManifest()
	t.NoError(err)
	t.True(found)

	ss := NewSyncers(local.Node(), local.Storage(), local.BlockFS(), local.Policy(), baseManifest)

	finishedChan := make(chan base.Height)
	ss.WhenFinished(func(height base.Height) {
		finishedChan <- height
	})
	t.NoError(ss.Start())

	defer ss.Stop()

	t.NoError(ss.Add(target-3, []network.Node{remote.Node()}))

	select {
	case <-time.After(time.Second * 3):
		t.NoError(xerrors.Errorf("timeout to wait to be finished"))
	case height := <-finishedChan:
		t.Equal(target-3, height)
		break
	}

	t.NoError(ss.Add(target, []network.Node{remote.Node()}))

	select {
	case <-time.After(time.Second * 3):
		t.NoError(xerrors.Errorf("timeout to wait to be finished"))
	case height := <-finishedChan:
		t.Equal(target, height)
		break
	}
}

func TestSyncers(t *testing.T) {
	suite.Run(t, new(testSyncers))
}
