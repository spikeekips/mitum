package mongodbstorage

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/base/block"
	"github.com/spikeekips/mitum/base/state"
	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/tree"
	"github.com/spikeekips/mitum/util/valuehash"
)

type BlockStorage struct {
	st              *Storage
	ost             *Storage
	block           block.Block
	operations      tree.FixedTree
	states          []state.State
	manifestModels  []mongo.WriteModel
	operationModels []mongo.WriteModel
	stateModels     []mongo.WriteModel
	statesValue     *sync.Map
}

func NewBlockStorage(st *Storage, blk block.Block) (*BlockStorage, error) {
	var nst *Storage
	if n, err := st.New(); err != nil {
		return nil, err
	} else {
		nst = n
	}

	bst := &BlockStorage{
		st:          nst,
		ost:         st,
		block:       blk,
		statesValue: &sync.Map{},
	}

	return bst, nil
}

func (bst *BlockStorage) Block() block.Block {
	return bst.block
}

func (bst *BlockStorage) SetBlock(ctx context.Context, blk block.Block) error {
	finished := make(chan error)
	go func() {
		finished <- bst.setBlock(blk)
	}()

	select {
	case <-ctx.Done():
		if err := bst.st.CleanByHeight(blk.Height()); err != nil {
			if !xerrors.Is(err, storage.NotFoundError) {
				return err
			}
		}

		return ctx.Err()
	case err := <-finished:
		return err
	}
}

func (bst *BlockStorage) setBlock(blk block.Block) error {
	startedf := time.Now()
	defer func() {
		bst.statesValue.Store("set-block", time.Since(startedf))
	}()

	if bst.block.Height() != blk.Height() {
		return xerrors.Errorf(
			"block has different height from initial block; initial=%d != block=%d",
			bst.block.Height(),
			blk.Height(),
		)
	}

	if bst.block.Round() != blk.Round() {
		return xerrors.Errorf(
			"block has different round from initial block; initial=%d != block=%d",
			bst.block.Round(),
			blk.Round(),
		)
	}

	if bst.manifestModels != nil {
		return nil
	}

	enc := bst.st.enc

	started := time.Now()
	if doc, err := NewManifestDoc(blk.Manifest(), enc); err != nil {
		return err
	} else {
		bst.statesValue.Store("set-manifest-model", time.Since(started))
		bst.manifestModels = append(bst.manifestModels, mongo.NewInsertOneModel().SetDocument(doc))
	}

	if err := bst.setOperationsTree(blk.OperationsTree()); err != nil {
		return err
	}

	if err := bst.setStates(blk.States()); err != nil {
		return err
	}

	bst.block = blk

	return nil
}

func (bst *BlockStorage) Commit(ctx context.Context) error {
	if err := bst.commit(ctx); err == nil {
		return nil
	} else {
		defer func() {
			_ = bst.Close()

			started := time.Now()
			bst.statesValue.Store("commit", time.Since(started))
		}()

		var me mongo.CommandError
		if xerrors.Is(err, context.DeadlineExceeded) {
			return storage.TimeoutError.Wrap(err)
		} else if xerrors.As(err, &me) {
			if me.HasErrorLabel("NetworkError") {
				return storage.TimeoutError.Wrap(err)
			}
		}

		return err
	}
}

func (bst *BlockStorage) commit(ctx context.Context) error {
	started := time.Now()
	defer func() {
		bst.statesValue.Store("commit", time.Since(started))

		_ = bst.Close()
	}()

	if bst.manifestModels == nil {
		if err := bst.SetBlock(ctx, bst.block); err != nil {
			return err
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	if res, err := bst.writeModels(ctx, ColNameManifest, bst.manifestModels); err != nil {
		return storage.WrapStorageError(err)
	} else if res != nil && res.InsertedCount < 1 {
		return xerrors.Errorf("manifest not inserted")
	}

	if res, err := bst.writeModels(ctx, ColNameOperation, bst.operationModels); err != nil {
		return storage.WrapStorageError(err)
	} else if res != nil && res.InsertedCount < 1 {
		return xerrors.Errorf("operation not inserted")
	}

	if res, err := bst.writeModels(ctx, ColNameState, bst.stateModels); err != nil {
		return storage.WrapStorageError(err)
	} else if res != nil && res.InsertedCount < 1 {
		return xerrors.Errorf("state not inserted")
	}

	if err := bst.ost.setLastBlock(bst.block, true, false); err != nil {
		return err
	}

	bst.insertCaches()

	return nil
}

func (bst *BlockStorage) setOperationsTree(tr tree.FixedTree) error {
	started := time.Now()
	defer func() {
		bst.statesValue.Store("set-operations-tree", time.Since(started))
	}()

	if tr.IsEmpty() {
		return nil
	}

	var models []mongo.WriteModel
	if err := tr.Traverse(func(_ int, key, _, _ []byte) (bool, error) {
		doc, err := NewOperationDoc(valuehash.NewBytes(key), bst.st.enc, bst.block.Height())
		if err != nil {
			return false, err
		}
		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))

		return true, nil
	}); err != nil {
		return err
	}

	bst.operationModels = models
	bst.operations = tr

	return nil
}

func (bst *BlockStorage) setStates(sts []state.State) error {
	started := time.Now()
	defer func() {
		bst.statesValue.Store("set-states", time.Since(started))
	}()

	var models []mongo.WriteModel
	for i := range sts {
		doc, err := NewStateDoc(sts[i], bst.st.enc)
		if err != nil {
			return err
		}
		models = append(models, mongo.NewInsertOneModel().SetDocument(doc))
	}

	bst.stateModels = models
	bst.states = sts

	return nil
}

func (bst *BlockStorage) writeModels(ctx context.Context, col string, models []mongo.WriteModel) (*mongo.BulkWriteResult, error) {
	started := time.Now()
	defer func() {
		bst.statesValue.Store(fmt.Sprintf("write-models-%s", col), time.Since(started))
	}()

	if len(models) < 1 {
		return nil, nil
	}

	return writeBulkModels(
		ctx,
		bst.st.client,
		col,
		models,
		defaultLimitWriteModels,
		options.BulkWrite().SetOrdered(false),
	)
}

func (bst *BlockStorage) insertCaches() {
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()

		if !bst.operations.IsEmpty() {
			_ = bst.operations.Traverse(func(_ int, key, _, _ []byte) (bool, error) {
				_ = bst.ost.operationFactCache.Set(valuehash.NewBytes(key).String(), struct{}{}, 0)

				return true, nil
			})
		}
	}()

	go func() {
		defer wg.Done()

		for i := range bst.states {
			_ = bst.ost.stateCache.Set(bst.states[i].Key(), bst.states[i], 0)
		}
	}()

	wg.Wait()
}

func (bst *BlockStorage) States() map[string]interface{} {
	m := map[string]interface{}{}
	bst.statesValue.Range(func(key, value interface{}) bool {
		m[key.(string)] = value

		return true
	})

	return m
}

func (bst *BlockStorage) Cancel() error {
	defer func() {
		_ = bst.Close()
	}()

	if bst.block == nil {
		return xerrors.Errorf("empty block")
	}

	return bst.st.CleanByHeight(bst.block.Height())
}

func (bst *BlockStorage) Close() error {
	bst.block = nil
	bst.states = nil
	bst.manifestModels = nil
	bst.operationModels = nil
	bst.stateModels = nil

	return bst.st.Close()
}
