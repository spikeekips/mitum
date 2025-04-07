package main

import (
	"context"
	"iter"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	"github.com/spikeekips/mitum/util"
	"github.com/valkey-io/valkey-go"
)

type valkeyDB struct {
	enc    encoder.Encoder
	client valkey.Client
}

func (db *valkeyDB) ping(ctx context.Context) error {
	return errors.WithStack(db.client.Do(ctx, db.client.B().Ping().Build()).Error())
}

func (db *valkeyDB) flushAll(ctx context.Context) error {
	return errors.WithStack(db.client.Do(ctx, db.client.B().Flushall().Build()).Error())
}

func (db *valkeyDB) saveOperationFacts(ctx context.Context, semSize, bulkSize int64, ops []dop5.OperationFact) error {
	return valkeyBulkSave(
		ctx,
		db.enc,
		db.client,
		semSize, bulkSize,
		ops,
		func(builder valkey.Builder, item dop5.OperationFact, b []byte) valkey.Completed {
			return builder.Setnx().
				Key(valkeyOperationFactKey(item.Hash())).
				Value(string(b)).
				Build()
		},
	)
}

func (db *valkeyDB) getOperationFact(ctx context.Context, facth util.Hash, fact *dop5.OperationFact) (bool, error) {
	return valkeyGet(ctx, db.enc, db.client, valkeyOperationFactKey(facth), fact)
}

func (db *valkeyDB) saveStates(ctx context.Context, semSize, bulkSize int64, states []base.State) error {
	return valkeyBulkSave(
		ctx,
		db.enc,
		db.client,
		semSize, bulkSize,
		states,
		func(builder valkey.Builder, st base.State, b []byte) valkey.Completed {
			return builder.Set().
				Key(valkeyStateKey(st.Key())).
				Value(string(b)).
				Build()
		},
	)
}

func (db *valkeyDB) getState(ctx context.Context, stateKey string, st *base.State) (bool, error) {
	return valkeyGet(ctx, db.enc, db.client, valkeyStateKey(stateKey), st)
}

func (db *valkeyDB) getStates(ctx context.Context) iter.Seq2[base.State, error] {
	return func(yield func(base.State, error) bool) {
		var cursor uint64

		for {
			entry, err := db.client.Do(ctx, db.client.B().Scan().Cursor(cursor).Match("st-*").Build()).AsScanEntry()
			if err != nil {
				yield(nil, err)

				return
			}

			if len(entry.Elements) < 1 {
				return
			}

			cursor = entry.Cursor

			vs, err := db.client.Do(ctx, db.client.B().Mget().Key(entry.Elements...).Build()).ToArray()
			if err != nil {
				yield(nil, err)

				return
			}

			switch i := len(vs); {
			case i < 1:
				yield(nil, errors.Errorf("empty"))

				return
			case i != len(entry.Elements):
				yield(nil, errors.Errorf("insufficient"))

				return
			}

			for i := range slices.Values(vs) {
				b, err := i.AsBytes()
				if err != nil {
					yield(nil, err)

					return
				}

				var st base.State

				if !yield(st, encoder.Unmarshal(db.enc, b, &st)) {
					return
				}
			}

			if entry.Cursor == 0 {
				return
			}
		}
	}
}

func valkeyOperationFactKey(facth util.Hash) string {
	return "op-" + facth.String()
}

func valkeyStateKey(stateKey string) string {
	return "st-" + stateKey
}

func valkeyBulkSave[T any](
	ctx context.Context,
	enc encoder.Encoder,
	client valkey.Client,
	semSize, bulkSize int64,
	items []T,
	setPipe func(valkey.Builder, T, []byte) valkey.Completed,
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
			e := i + bulkSize
			if e > size {
				e = size
			}

			cmds := make(valkey.Commands, e-i)

			for j, item := range slices.All(items[i:e]) {
				switch b, err := enc.Marshal(item); {
				case err != nil:
					return err
				default:
					cmds[j] = setPipe(client.B(), item, b)
				}
			}

			for res := range slices.Values(client.DoMulti(ctx, cmds...)) {
				if err := res.Error(); err != nil {
					return errors.WithStack(err)
				}
			}

			return nil
		}); err != nil {
			return jobWait(err)
		}
	}

	jobDone()

	return jobWait(nil)
}

func valkeyGet[T any](
	ctx context.Context,
	enc encoder.Encoder,
	client valkey.Client,
	key string,
	t *T,
) (bool, error) {
	switch s, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString(); {
	case err != nil && errors.Is(err, valkey.Nil):
		return false, nil
	case err != nil:
		return false, errors.WithStack(err)
	default:
		return true, encoder.Unmarshal(enc, []byte(s), t)
	}
}
