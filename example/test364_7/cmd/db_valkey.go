package main

import (
	"context"
	"iter"
	"slices"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dop7 "github.com/spikeekips/mitum/example/test364_7"
	"github.com/spikeekips/mitum/util"
	"github.com/valkey-io/valkey-go"
)

type valkeyIncompleteBuilder interface {
	Build() valkey.Completed
}

func valkeyDoError(ctx context.Context, client valkey.Client, cmd valkeyIncompleteBuilder) error {
	return errors.WithStack(client.Do(ctx, cmd.Build()).Error())
}

func valkeyPing(ctx context.Context, client valkey.Client) error {
	return valkeyDoError(ctx, client, client.B().Ping())
}

func valkeyFlushAll(ctx context.Context, client valkey.Client) error {
	return valkeyDoError(ctx, client, client.B().Flushall())
}

func valkeySaveOperationFacts(ctx context.Context, client valkey.Client, enc encoder.Encoder, semSize, bulkSize uint64, ops []dop5.OperationFact) error {
	return valkeyBulkSave(
		ctx,
		enc,
		client,
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

func valkeyGetOperationFact(ctx context.Context, client valkey.Client, enc encoder.Encoder, facth util.Hash, fact *dop5.OperationFact) (bool, error) {
	return valkeyGet(ctx, enc, client, valkeyOperationFactKey(facth), fact)
}

func valkeySaveStates(ctx context.Context, client valkey.Client, enc encoder.Encoder, semSize, bulkSize uint64, states []base.State) error {
	return valkeyBulkSave(
		ctx,
		enc,
		client,
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

func valkeyGetState(ctx context.Context, client valkey.Client, enc encoder.Encoder, stateKey string, st *base.State) (bool, error) {
	return valkeyGet(ctx, enc, client, valkeyStateKey(stateKey), st)
}

func valkeyGetStates(ctx context.Context, client valkey.Client, enc encoder.Encoder) iter.Seq2[base.State, error] {
	return func(yield func(base.State, error) bool) {
		var cursor uint64

		for {
			entry, err := client.Do(ctx, client.B().Scan().Cursor(cursor).Match("st-*").Build()).AsScanEntry()
			if err != nil {
				yield(nil, err)

				return
			}

			if len(entry.Elements) < 1 {
				return
			}

			cursor = entry.Cursor

			vs, err := client.Do(ctx, client.B().Mget().Key(entry.Elements...).Build()).ToArray()
			if err != nil {
				yield(nil, err)

				return
			}

			switch i := len(vs); {
			case i < 1:
				yield(nil, errors.New("empty"))

				return
			case i != len(entry.Elements):
				yield(nil, errors.New("insufficient"))

				return
			}

			for i := range slices.Values(vs) {
				b, err := i.AsBytes()
				if err != nil {
					yield(nil, err)

					return
				}

				var st base.State

				if !yield(st, encoder.Unmarshal(enc, b, &st)) {
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
	semSize, bulkSize uint64,
	items []T,
	setPipe func(valkey.Builder, T, []byte) valkey.Completed,
) error {
	switch {
	case len(items) < 1:
		return nil
	case semSize < 1:
		return errors.New("zero sem size")
	case bulkSize < 1:
		return errors.New("zero bulk size")
	}

	newJob, jobDone, jobWait := dop7.NewFuncJobWorker(ctx, semSize)

	size := uint64(len(items))

	for i := uint64(0); i < size; i += bulkSize {
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
