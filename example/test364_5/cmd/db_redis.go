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
	"github.com/spikeekips/mitum/util"
)

type redisDB struct {
	enc encoder.Encoder
	rdb *redis.Client
}

func (db *redisDB) ping(ctx context.Context) error {
	if _, err := db.rdb.Ping(ctx).Result(); err != nil {
		return errors.WithMessage(err, "redis")
	}

	return nil
}

func (db *redisDB) flushAll(ctx context.Context) error {
	return errors.WithStack(db.rdb.FlushAll(ctx).Err())
}

func (db *redisDB) saveOperationFacts(ctx context.Context, semSize, bulkSize int64, ops []dop5.OperationFact) error {
	return redisBulkSave(
		ctx,
		db.enc,
		db.rdb,
		semSize, bulkSize,
		ops,
		func(ctx context.Context, pipe redis.Pipeliner, item dop5.OperationFact, b []byte) {
			_ = pipe.SetNX(ctx, redisOperationFactKey(item), b, 0)
		},
	)
}

func (db *redisDB) getOperationFact(ctx context.Context, oph util.Hash, fact *dop5.OperationFact) (bool, error) {
	return redisGet(ctx, db.enc, db.rdb, "op-"+oph.String(), fact)
}

func (db *redisDB) saveStates(ctx context.Context, semSize, bulkSize int64, states []base.State) error {
	return redisBulkSave(
		ctx,
		db.enc,
		db.rdb,
		semSize, bulkSize,
		states,
		func(ctx context.Context, pipe redis.Pipeliner, st base.State, b []byte) {
			_ = pipe.Set(ctx, "st-"+st.Key(), b, 0)
		},
	)
}

func (db *redisDB) getState(ctx context.Context, stateKey string, st *base.State) (bool, error) {
	return redisGet(ctx, db.enc, db.rdb, "st-"+stateKey, st)
}

var defaultLimit = 1 << 9

func (db *redisDB) getStates(ctx context.Context) iter.Seq2[base.State, error] {
	return func(yield func(base.State, error) bool) {
		cmd := db.rdb.Scan(ctx, 0, "st-*", 0)
		if _, _, err := cmd.Result(); err != nil {
			yield(nil, err)

			return
		}

		it := cmd.Iterator()

		for {
			switch l, err := db.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for range defaultLimit {
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
					if !yield(st, encoder.Unmarshal(db.enc, []byte(strcmd.Val()), &st)) {
						return
					}
				}
			}
		}
	}
}

func redisOperationFactKey(fact dop5.OperationFact) string {
	return "op-" + fact.Hash().String()
}

func redisBulkSave[T any](
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	semSize, bulkSize int64,
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

	newJob, jobDone, jobWait, err := defaultNewFuncJobWorker(ctx, semSize)
	if err != nil {
		return err
	}

	size := int64(len(items))

	for i := int64(0); i < size; i += bulkSize {
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

			return errors.WithStack(err)
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
		return false, errors.WithStack(err)
	default:
		return true, encoder.Unmarshal(enc, []byte(c.Val()), t)
	}
}
