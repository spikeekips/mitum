package main

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/valuehash"
)

func (t *testHandlers) TestManagerStateOwners() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stateKey := util.UUID().String()
		owners := []string{
			util.UUID().String(),
			util.UUID().String(),
			util.UUID().String(),
			util.UUID().String(),
		}

		handler, err := newManagerHandlerStateOwnersFunc(func(base.Height) (func(string) ([]string, error), error) {
			return func(k string) ([]string, error) {
				if k != stateKey {
					return nil, util.ErrNotFound.Errorf("unknown state key")
				}

				return owners, nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerStateOwnersHeaderHint), handler,
			t.ci,
			newManagerBrokerStateOwnersFunc(33, "w0", stateKey, func(os []string) error {
				t.Equal(owners, os)

				return nil
			}),
		))
	})

	t.Run("empty owners", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handler, err := newManagerHandlerStateOwnersFunc(func(base.Height) (func(string) ([]string, error), error) {
			return func(k string) ([]string, error) {
				return nil, nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerStateOwnersHeaderHint), handler,
			t.ci,
			newManagerBrokerStateOwnersFunc(33, "w0", util.UUID().String(), func(os []string) error {
				t.Empty(os)

				return nil
			}),
		))
	})
}

func (t *testHandlers) TestManagerWorkerCanceled() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cancelch := make(chan string, 1)
		handler, err := newManagerHandlerWorkerCanceledFunc(func(base.Height) (func(string) error, error) {
			return func(worker string) error {
				cancelch <- worker

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerCanceledHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerCanceledFunc(33, "w0"),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case <-cancelch:
		}
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cancelch := make(chan error, 1)
		handler, err := newManagerHandlerWorkerCanceledFunc(func(base.Height) (func(string) error, error) {
			err := errors.Errorf("findme")
			cancelch <- err

			return nil, err
		})
		t.NoError(err)

		header := NewManagerWorkerCanceledHeader(33, "w0")

		err = requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerCanceledHeaderHint), handler,
			t.ci,
			func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
				if err := broker.WriteRequestHead(ctx, header); err != nil {
					return err
				}

				_, res, err := broker.ReadResponseHead(ctx)
				if err != nil {
					return err
				}

				return res.Err()
			},
		)
		t.ErrorContains(err, "findme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case err = <-cancelch:
			t.ErrorContains(err, "findme")
		}
	})
}

func (t *testHandlers) TestManagerWorkerUpdateMerged() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stateKeys := make([]string, 33)
		stateHashes := make([]util.Hash, len(stateKeys))

		for i := range stateKeys {
			stateKeys[i] = util.UUID().String()
			stateHashes[i] = valuehash.RandomSHA256()
		}

		ops := make([]uint64, 11)
		reasons := make([]string, len(ops))
		for i := range ops {
			ops[i] = uint64(i)
			reasons[i] = util.UUID().String()
		}

		adds, dones, _ := util.CompactAppendSliceV1[string](33, 33)
		addh, doneh, _ := util.CompactAppendSliceV1[util.Hash](33, 33)
		addo, doneo, _ := util.CompactAppendSliceV1[uint64](33, 33)
		addr, doner, _ := util.CompactAppendSliceV1[string](33, 33)

		donech := make(chan struct{}, 1)

		handler, err := newManagerHandlerWorkerUpdateMergedFunc(func(height base.Height, worker string) (
			func(stateKey string, stateHash util.Hash) error,
			func(opindex uint64, reason string) error,
			func() error,
			error,
		) {
			return func(stateKey string, stateHash util.Hash) error {
					adds(stateKey)
					addh(stateHash)

					return nil
				},
				func(opindex uint64, reason string) error {
					addo(opindex)
					addr(reason)

					return nil
				},
				func() error {
					donech <- struct{}{}

					return nil
				},
				nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerUpdateMergedHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerUpdateMergedFunc(33, "w0",
				func(f func(stateKey string, stateHash util.Hash) error) error {
					return util.TraverseSliceV1(stateKeys, func(i int, k string) (bool, error) {
						return true, f(stateKeys[i], stateHashes[i])
					})
				},
				func(f func(opindex uint64, reason string) error) error {
					return util.TraverseSliceV1(ops, func(i int, k uint64) (bool, error) {
						return true, f(ops[i], reasons[i])
					})
				},
			),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case <-donech:
		}

		dones(func(s []string) error { t.Equal(stateKeys, s); return nil })
		doneh(func(s []util.Hash) error {
			t.Equal(len(stateHashes), len(s))

			util.TraverseSliceV1(stateHashes, func(i int, h util.Hash) (bool, error) {
				t.True(h.Equal(s[i]))

				return true, nil
			})
			return nil
		})
		doneo(func(s []uint64) error { t.Equal(ops, s); return nil })
		doner(func(s []string) error { t.Equal(reasons, s); return nil })
	})

	t.Run("empty states", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := make([]uint64, 11)
		reasons := make([]string, len(ops))
		for i := range ops {
			ops[i] = uint64(i)
			reasons[i] = util.UUID().String()
		}

		adds, dones, _ := util.CompactAppendSliceV1[string](33, 33)
		addh, doneh, _ := util.CompactAppendSliceV1[util.Hash](33, 33)
		addo, doneo, _ := util.CompactAppendSliceV1[uint64](33, 33)
		addr, doner, _ := util.CompactAppendSliceV1[string](33, 33)

		donech := make(chan struct{}, 1)

		handler, err := newManagerHandlerWorkerUpdateMergedFunc(func(height base.Height, worker string) (
			func(stateKey string, stateHash util.Hash) error,
			func(opindex uint64, reason string) error,
			func() error,
			error,
		) {
			return func(stateKey string, stateHash util.Hash) error {
					adds(stateKey)
					addh(stateHash)

					return nil
				},
				func(opindex uint64, reason string) error {
					addo(opindex)
					addr(reason)

					return nil
				},
				func() error {
					donech <- struct{}{}

					return nil
				},
				nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerUpdateMergedHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerUpdateMergedFunc(33, "w0",
				func(func(stateKey string, stateHash util.Hash) error) error {
					return nil
				},
				func(f func(opindex uint64, reason string) error) error {
					return util.TraverseSliceV1(ops, func(i int, k uint64) (bool, error) {
						return true, f(ops[i], reasons[i])
					})
				},
			),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case <-donech:
		}

		dones(func(s []string) error { t.Empty(s); return nil })
		doneh(func(s []util.Hash) error { t.Empty(s); return nil })
		doneo(func(s []uint64) error { t.Equal(ops, s); return nil })
		doner(func(s []string) error { t.Equal(reasons, s); return nil })
	})

	t.Run("empty reasons", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		stateKeys := make([]string, 33)
		stateHashes := make([]util.Hash, len(stateKeys))

		for i := range stateKeys {
			stateKeys[i] = util.UUID().String()
			stateHashes[i] = valuehash.RandomSHA256()
		}

		adds, dones, _ := util.CompactAppendSliceV1[string](33, 33)
		addh, doneh, _ := util.CompactAppendSliceV1[util.Hash](33, 33)
		addo, doneo, _ := util.CompactAppendSliceV1[uint64](33, 33)
		addr, doner, _ := util.CompactAppendSliceV1[string](33, 33)

		donech := make(chan struct{}, 1)

		handler, err := newManagerHandlerWorkerUpdateMergedFunc(func(height base.Height, worker string) (
			func(stateKey string, stateHash util.Hash) error,
			func(opindex uint64, reason string) error,
			func() error,
			error,
		) {
			return func(stateKey string, stateHash util.Hash) error {
					adds(stateKey)
					addh(stateHash)

					return nil
				},
				func(opindex uint64, reason string) error {
					addo(opindex)
					addr(reason)

					return nil
				},
				func() error {
					donech <- struct{}{}

					return nil
				},
				nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerUpdateMergedHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerUpdateMergedFunc(33, "w0",
				func(f func(stateKey string, stateHash util.Hash) error) error {
					return util.TraverseSliceV1(stateKeys, func(i int, k string) (bool, error) {
						return true, f(stateKeys[i], stateHashes[i])
					})
				},
				func(f func(opindex uint64, reason string) error) error {
					return nil
				},
			),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case <-donech:
		}

		dones(func(s []string) error { t.Equal(stateKeys, s); return nil })
		doneh(func(s []util.Hash) error {
			t.Equal(len(stateHashes), len(s))

			util.TraverseSliceV1(stateHashes, func(i int, h util.Hash) (bool, error) {
				t.True(h.Equal(s[i]))

				return true, nil
			})
			return nil
		})
		doneo(func(s []uint64) error { t.Empty(s); return nil })
		doner(func(s []string) error { t.Empty(s); return nil })
	})
}

func (t *testHandlers) TestManagerWorkerStoreDone() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan struct{}, 1)

		chunks := map[base.BlockItemType]string{
			base.BlockItemMap:            util.UUID().String(),
			base.BlockItemProposal:       util.UUID().String(),
			base.BlockItemOperations:     util.UUID().String(),
			base.BlockItemOperationsTree: util.UUID().String(),
		}

		handler, err := newManagerHandlerWorkerStoreDoneFunc(func(height base.Height) (
			func(worker string, chunks map[base.BlockItemType]string) error,
			error,
		) {
			return func(worker string, rchunks map[base.BlockItemType]string) error {
				t.Equal("w0", worker)
				t.Equal(chunks, rchunks)

				donech <- struct{}{}

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerStoreDoneHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerStoreDoneFunc(33, "w0", chunks),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case <-donech:
		}
	})

	t.Run("empty chunks", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan struct{}, 1)

		handler, err := newManagerHandlerWorkerStoreDoneFunc(func(height base.Height) (
			func(worker string, chunks map[base.BlockItemType]string) error,
			error,
		) {
			return func(worker string, rchunks map[base.BlockItemType]string) error {
				t.Equal("w0", worker)
				t.Empty(rchunks)

				donech <- struct{}{}

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerStoreDoneHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerStoreDoneFunc(33, "w0", nil),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case <-donech:
		}
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan error, 1)

		handler, err := newManagerHandlerWorkerStoreDoneFunc(func(height base.Height) (
			func(worker string, chunks map[base.BlockItemType]string) error,
			error,
		) {
			return func(worker string, rchunks map[base.BlockItemType]string) error {
				err := errors.Errorf("showme")

				donech <- err

				return err
			}, nil
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(ManagerWorkerStoreDoneHeaderHint), handler,
			t.ci,
			newManagerBrokerWorkerStoreDoneFunc(33, "w0", nil),
		)
		t.ErrorContains(err, "showme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("wait done")
		case err = <-donech:
			t.ErrorContains(err, "showme")
		}
	})
}
