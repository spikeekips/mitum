package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

func redisOperationFactKey(fact dop5.OperationFact) string {
	return "op-" + fact.Hash().String()
}

func redisSaveOperationFacts(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	semSize, bulkSize uint64,
	ops []dop5.OperationFact,
) error {
	return redisBulkSave(
		ctx,
		enc,
		rdb,
		semSize, bulkSize,
		ops,
		redisOperationFactKey,
	)
}

func redisGetOperationFact(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	h util.Hash,
	fact *dop5.OperationFact,
) (bool, error) {
	return redisGet(ctx, enc, rdb, "op-"+h.String(), fact)
}

func redisStateKey(st base.State) string {
	return "st-" + st.Key()
}

func redisSaveStates(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	semSize, bulkSize uint64,
	states []base.State,
) error {
	return redisBulkSave(
		ctx,
		enc,
		rdb,
		semSize, bulkSize,
		states,
		redisStateKey,
	)
}

func redisGetState(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	stateKey string,
	st *base.State,
) (bool, error) {
	return redisGet(ctx, enc, rdb, "st-"+stateKey, st)
}

func redisBulkSave[T any](
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	semSize, bulkSize uint64,
	items []T,
	keyf func(T) string,
) error {
	switch {
	case len(items) < 1:
		return nil
	case semSize < 1:
		return errors.Errorf("zero sem size")
	case bulkSize < 1:
		return errors.Errorf("zero bulk size")
	}

	newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(ctx, semSize)
	if err != nil {
		return err
	}

	size := uint64(len(items))

	for i := uint64(0); i < size; i += bulkSize {
		if err := newJob(func(ctx context.Context) error {
			_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				e := i + bulkSize
				if e > size {
					e = size
				}

				for item := range slices.Values(items[i:e]) {
					switch b, err := enc.Marshal(item); {
					case err != nil:
						return err
					default:
						_ = pipe.SetNX(ctx, keyf(item), b, 0)
					}
				}

				return nil
			})

			return err
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func redisGet[T any](
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	key string,
	t *T,
) (bool, error) {
	c := rdb.Get(ctx, key)

	switch err := c.Err(); {
	case err != nil && errors.Is(err, redis.Nil):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, encoder.Unmarshal(enc, []byte(c.Val()), t)
	}
}
