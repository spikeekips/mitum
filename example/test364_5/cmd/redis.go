package main

import (
	"context"
	"iter"
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
		func(ctx context.Context, pipe redis.Pipeliner, item dop5.OperationFact, b []byte) {
			_ = pipe.SetNX(ctx, redisOperationFactKey(item), b, 0)
		},
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
		func(ctx context.Context, pipe redis.Pipeliner, st base.State, b []byte) {
			_ = pipe.SetNX(ctx, "st-"+st.Key(), b, 0)
		},
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

func redisGetStates(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
) iter.Seq2[base.State, error] {
	limit := 1 << 9

	return func(yield func(base.State, error) bool) {
		cmd := rdb.Scan(ctx, 0, "st-*", 1<<9)
		if _, _, err := cmd.Result(); err != nil {
			yield(nil, err)

			return
		}

		it := cmd.Iterator()

		for {
			switch l, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for i := 0; i < limit; i++ {
					if !it.Next(ctx) {
						return nil
					}

					_ = pipe.Get(ctx, it.Val())
				}

				return nil
			}); {
			case err != nil:
				yield(nil, err)

				return
			case len(l) < 1:
				return
			default:
				for cmd := range slices.Values(l) {
					strcmd, ok := cmd.(*redis.StringCmd)
					if !ok {
						yield(nil, errors.Errorf("expected *redis.StringCmd, but %T", cmd))
					}

					var st base.State
					if !yield(st, encoder.Unmarshal(enc, []byte(strcmd.Val()), &st)) {
						return
					}
				}
			}
		}
	}
}

func redisBulkSave[T any](
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	semSize, bulkSize uint64,
	items []T,
	setPipe func(context.Context, redis.Pipeliner, T, []byte),
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
						setPipe(ctx, pipe, item, b)
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
