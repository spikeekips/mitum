package main

import (
	"context"
	"slices"

	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

func saveOperationFacts(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	ops []dop5.OperationFact,
	semSize, bulkSize uint64,
) error {
	switch {
	case semSize < 1:
		return errors.Errorf("zero sem size")
	case bulkSize < 1:
		return errors.Errorf("zero bulk size")
	}

	newJob, jobDone, jobWait, err := dutil.NewFuncJobWorker(ctx, semSize)
	if err != nil {
		return err
	}

	size := uint64(len(ops))
	for i := uint64(0); i < size; i += bulkSize {
		e := i + bulkSize
		if e > size {
			e = size
		}

		if err := newJob(func(ctx context.Context) error {
			_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
				for fact := range slices.Values(ops[i:e]) {
					switch b, err := enc.Marshal(fact); {
					case err != nil:
						return err
					default:
						_ = pipe.SetNX(ctx, fact.Hash().String(), b, 0)
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

func getOperationFact(
	ctx context.Context,
	enc encoder.Encoder,
	rdb *redis.Client,
	h util.Hash,
	fact *dop5.OperationFact,
) (bool, error) {
	c := rdb.Get(ctx, h.String())

	switch err := c.Err(); {
	case err != nil && errors.Is(err, redis.Nil):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, encoder.Decode(enc, []byte(c.Val()), fact)
	}
}
