package main

import (
	"cmp"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacdatabase "github.com/spikeekips/mitum/isaac/database"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/valuehash"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

func requestFunc[T quicstreamheader.RequestHeader](
	ctx context.Context,
	encs *encoder.Encoders,
	name quicstream.HandlerName,
	handler quicstreamheader.Handler[T],
	ci quicstream.ConnInfo,
	f quicstreamheader.BrokerFunc,
) error {
	_, dialf := isaacnetwork.TestingDialFunc(encs, name, handler)

	hdialf := quicstreamheader.NewDialFunc(dialf, encs, encs.Default())

	stream, closef, err := hdialf(ctx, ci)
	if err != nil {
		return err
	}

	defer closef()

	return stream(ctx, f)
}

type testHandlers struct {
	isaacdatabase.BaseTestDatabase
	isaac.BaseTestBallots
	baseTestAccountStateMerger
	ci quicstream.ConnInfo
}

func (t *testHandlers) SetupTest() {
	t.BaseTestBallots.SetupTest()
	t.BaseTestDatabase.SetupTest()
}

func (t *testHandlers) SetupSuite() {
	t.BaseTestDatabase.SetupSuite()

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: AccountStateValueHint, Instance: AccountStateValue{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: AccountStateMergeValueHint, Instance: AccountStateMergeValue{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationFactHint, Instance: isaac.DummyOperationFact{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: isaac.DummyOperationHint, Instance: isaac.DummyOperation{}}))

	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: quicstreamheader.DefaultResponseHeaderHint, Instance: quicstreamheader.DefaultResponseHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerCancelHeaderHint, Instance: WorkerCancelHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerDoneHeaderHint, Instance: WorkerDoneHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerReadyHeaderHint, Instance: WorkerReadyHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerNewOperationsHeaderHint, Instance: WorkerNewOperationsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerInsertOpHeaderHint, Instance: WorkerInsertOpHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerInsertValuesDoneHeaderHint, Instance: WorkerInsertValuesDoneHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerPhaseDoneHeaderHint, Instance: WorkerPhaseDoneHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ManagerStateOwnersHeaderHint, Instance: ManagerStateOwnersHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ManagerWorkerCanceledHeaderHint, Instance: ManagerWorkerCanceledHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ManagerWorkerUpdateMergedHeaderHint, Instance: ManagerWorkerUpdateMergedHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: ManagerWorkerStoreDoneHeaderHint, Instance: ManagerWorkerStoreDoneHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerSetStatesHeaderHint, Instance: WorkerSetStatesHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerSetOperationsHeaderHint, Instance: WorkerSetOperationsHeader{}}))
	t.NoError(t.Enc.Add(encoder.DecodeDetail{Hint: WorkerSyncStatesHeaderHint, Instance: WorkerSyncStatesHeader{}}))

	t.ci = quicstream.UnsafeConnInfo(nil, true)
}

func (t *testHandlers) TestWorkerCancel() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		cancelch := make(chan struct{}, 1)
		handler, err := newWorkerHandlerCancelFunc(func(base.Height) (func(), error) {
			return func() { cancelch <- struct{}{} }, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerCancelHeaderHint), handler,
			t.ci,
			newWorkerBrokerCancelFunc(33),
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
		handler, err := newWorkerHandlerCancelFunc(func(base.Height) (func(), error) {
			err := errors.Errorf("findme")
			cancelch <- err

			return nil, err
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerCancelHeaderHint), handler,
			t.ci,
			newWorkerBrokerCancelFunc(33),
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

func (t *testHandlers) TestWorkerDone() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan struct{}, 1)
		handler, err := newWorkerHandlerWorkerDoneFunc(func(base.Height) (func() error, error) {
			return func() error {
				donech <- struct{}{}

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerDoneFunc(33),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case <-donech:
		}
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan error, 1)
		handler, err := newWorkerHandlerWorkerDoneFunc(func(base.Height) (func() error, error) {
			err := errors.Errorf("findme")
			donech <- err

			return func() error { return err }, nil
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerDoneFunc(33),
		)
		t.ErrorContains(err, "findme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case err = <-donech:
			t.ErrorContains(err, "findme")
		}
	})
}

func (t *testHandlers) TestWorkerReady() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		requestedch := make(chan processWorkerReadyArgs, 1)
		handler, err := newWorkerHandlerReadyFunc(func(args processWorkerReadyArgs) error {
			requestedch <- args
			return nil
		})
		t.NoError(err)

		args := processWorkerReadyArgs{
			Manager:     "findme",
			Height:      33,
			Workers:     []string{"a", "b", "c"},
			NStateShare: 3,
			NOps:        44,
		}

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerReadyHeaderHint), handler,
			t.ci,
			newWorkerBrokerReadyFunc(args),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case args := <-requestedch:
			t.Equal(args, args)
		}
	})

	t.Run("not ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		handler, err := newWorkerHandlerReadyFunc(func(processWorkerReadyArgs) error {
			return errors.Errorf("showme")
		})
		t.NoError(err)

		args := processWorkerReadyArgs{}

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerReadyHeaderHint), handler,
			t.ci,
			newWorkerBrokerReadyFunc(args),
		)

		t.ErrorContains(err, "showme")
	})
}

func (t *testHandlers) TestWorkerNewOperations() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ops := make([][2]util.Hash, 33)
		for i := range ops {
			ops[i] = [2]util.Hash{valuehash.RandomSHA256(), valuehash.RandomSHA256()}
		}

		add, done, err := util.CompactAppendSliceV1[[3]interface{}](uint64(len(ops)), 1<<3)
		t.NoError(err)

		var received [][2]util.Hash
		canceledch := make(chan error, 1)

		handler, err := newWorkerHandlerNewOperationsFunc(
			func(height base.Height) (
				workerNewOperationsFunc,
				func() error,
				func(error),
				error,
			) {
				return func(opindex uint64, op, fact util.Hash) error {
						add([3]interface{}{opindex, op, fact})

						return nil
					},
					func() error {
						return done(func(s [][3]interface{}) error {
							received = make([][2]util.Hash, len(s))

							util.TraverseSliceV1(s, func(_ int, v [3]interface{}) (bool, error) {
								received[v[0].(uint64)] = [2]util.Hash{v[1].(util.Hash), v[2].(util.Hash)}

								return true, nil
							})

							return nil
						})
					},
					func(err error) {
						canceledch <- err
					},
					nil
			},
		)
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerNewOperationsHeaderHint), handler,
			t.ci,
			newWorkerBrokerNewOperationsFunc(33, func(f func(opindex uint64, op, fact util.Hash) error) error {
				return util.TraverseSliceV1(ops, func(i int, v [2]util.Hash) (bool, error) {
					return true, f(uint64(i), v[0], v[1])
				})
			}),
		))

		select {
		case <-time.After(time.Second):
			t.Fail("not canceled")
		case err := <-canceledch:
			t.Nil(err)
		}

		t.Equal(len(ops), len(received))
		for i := range ops {
			t.Equal(ops[i][0].Bytes(), received[i][0].Bytes())
			t.Equal(ops[i][1].Bytes(), received[i][1].Bytes())
		}
	})

	t.Run("empty operations", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		add, done, err := util.CompactAppendSliceV1[uint64](1<<3, 1<<3)
		t.NoError(err)

		var received []uint64
		canceledch := make(chan error, 2)

		handler, err := newWorkerHandlerNewOperationsFunc(
			func(height base.Height) (
				workerNewOperationsFunc,
				func() error,
				func(error),
				error,
			) {
				return func(opindex uint64, op, fact util.Hash) error {
						add(opindex)

						return nil
					},
					func() error {
						return done(func(s []uint64) error {
							received = s

							return nil
						})
					},
					func(err error) {
						canceledch <- err
					},
					nil
			},
		)
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerNewOperationsHeaderHint), handler,
			t.ci,
			newWorkerBrokerNewOperationsFunc(33, nil),
		)
		t.ErrorContains(err, "empty operations")

		select {
		case <-time.After(time.Second):
			t.Fail("not canceled")
		case err := <-canceledch:
			t.ErrorContains(err, "empty operations")
		}

		t.Empty(received)
	})

	t.Run("cancel", func() {})
}

func (t *testHandlers) TestWorkerInsertOp() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		values := make([][]StateMergeValue, 33)
		for i := range values {
			values[i] = t.randomMergeValues(1, 2)
		}

		owners := make([]map[string][]string, len(values))
		for i := range owners {
			owners[i] = map[string][]string{}

			util.TraverseSliceV1(values[i], func(_ int, v StateMergeValue) (bool, error) {
				owners[i][v.Key()] = []string{util.UUID().String(), util.UUID().String()}

				return true, nil
			})
		}

		add, done, err := util.CompactAppendSliceV1[[5]interface{}](uint64(len(values)), 1<<9)
		t.NoError(err)

		handler, err := newWorkerHandlerInsertOpFunc(func(base.Height) (processWorkerInsertValues, error) {
			return func(worker string, opindex uint64, fact util.Hash, values []StateMergeValue, owners map[string][]string) error {
				add([5]interface{}{worker, opindex, fact, values, owners})

				return nil
			}, nil
		})
		t.NoError(err)

		facts := make([]util.Hash, len(values))

		for i := range values {
			opindex := uint64(i)
			fact := valuehash.RandomSHA256()
			vs := values[i]
			os := owners[i]

			t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerInsertOpHeaderHint), handler,
				t.ci,
				newWorkerBrokerInsertOpFunc(
					33,
					"w3",
					t.Encs.Default(),
					opindex,
					fact,
					vs,
					os,
				),
			))

			facts[i] = fact
		}

		_ = done(func(s [][5]interface{}) error {
			sort.Slice(s, func(i, j int) bool {
				return cmp.Less[uint64](
					s[i][1].(uint64),
					s[j][1].(uint64),
				)
			})

			t.Equal(len(values), len(s))

			for i := range values {
				b := s[i]

				t.Equal("w3", b[0])
				t.Equal(uint64(i), b[1])
				t.True(facts[i].Equal(b[2].(util.Hash)))

				avs := values[i]
				bvs := b[3].([]StateMergeValue)

				t.Equal(len(avs), len(bvs))

				for j := range avs {
					av := avs[j]
					bv := bvs[j]

					t.Equal(av.Key(), bv.Key())
					t.True(base.IsEqualStateValue(av.Value(), bv.Value()))
				}

				t.Equal(owners[i], b[4].(map[string][]string))
			}

			return nil
		})
	})
}

func (t *testHandlers) TestWorkerInsertValuesDone() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan string, 1)
		handler, err := newWorkerHandlerInsertValuesDoneFunc(func(base.Height) (func(string) error, error) {
			return func(worker string) error {
				donech <- worker

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerInsertValuesDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerInsertValuesDoneFunc(33, "w3"),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case worker := <-donech:
			t.Equal("w3", worker)
		}
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan error, 1)
		handler, err := newWorkerHandlerInsertValuesDoneFunc(func(base.Height) (func(string) error, error) {
			err := errors.Errorf("findme")
			donech <- err

			return func(string) error { return err }, nil
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerInsertValuesDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerInsertValuesDoneFunc(33, "w3"),
		)
		t.ErrorContains(err, "findme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case err = <-donech:
			t.ErrorContains(err, "findme")
		}
	})
}

func (t *testHandlers) TestWorkerPhaseDone() {
	mergedOps, unmergedOps := []uint64{1, 2, 3}, []uint64{4, 5, 6, 7}

	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var rphase int
		var rmergedOps, runmergedOps []uint64

		donech := make(chan string, 1)
		handler, err := newWorkerHandlerPhaseDoneFunc(func(base.Height) (
			func(string, int, []uint64, []uint64) error,
			error,
		) {
			return func(worker string, phase int, mergedOps, unmergedOps []uint64) error {
				rphase = phase
				rmergedOps = mergedOps
				runmergedOps = unmergedOps

				donech <- worker

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerPhaseDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerPhaseDoneFunc(33, "w3", 3, mergedOps, unmergedOps),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case worker := <-donech:
			t.Equal("w3", worker)
		}

		t.Equal(3, rphase)
		t.Equal(mergedOps, rmergedOps)
		t.Equal(unmergedOps, runmergedOps)
	})

	t.Run("empty mergedOps", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var rphase int
		var rmergedOps, runmergedOps []uint64

		donech := make(chan string, 1)
		handler, err := newWorkerHandlerPhaseDoneFunc(func(base.Height) (
			func(string, int, []uint64, []uint64) error,
			error,
		) {
			return func(worker string, phase int, mergedOps, unmergedOps []uint64) error {
				rphase = phase
				rmergedOps = mergedOps
				runmergedOps = unmergedOps

				donech <- worker

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerPhaseDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerPhaseDoneFunc(33, "w3", 3, nil, unmergedOps),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case worker := <-donech:
			t.Equal("w3", worker)
		}

		t.Equal(3, rphase)
		t.Empty(rmergedOps)
		t.Equal(unmergedOps, runmergedOps)
	})

	t.Run("empty unmergedOps", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var rphase int
		var rmergedOps, runmergedOps []uint64

		donech := make(chan string, 1)
		handler, err := newWorkerHandlerPhaseDoneFunc(func(base.Height) (
			func(string, int, []uint64, []uint64) error,
			error,
		) {
			return func(worker string, phase int, mergedOps, unmergedOps []uint64) error {
				rphase = phase
				rmergedOps = mergedOps
				runmergedOps = unmergedOps

				donech <- worker

				return nil
			}, nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerPhaseDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerPhaseDoneFunc(33, "w3", 3, mergedOps, nil),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case worker := <-donech:
			t.Equal("w3", worker)
		}

		t.Equal(3, rphase)
		t.Equal(mergedOps, rmergedOps)
		t.Empty(runmergedOps)
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan error, 1)
		handler, err := newWorkerHandlerPhaseDoneFunc(func(base.Height) (
			func(string, int, []uint64, []uint64) error,
			error,
		) {
			return func(worker string, _ int, _, _ []uint64) error {
				err := errors.Errorf("findme")
				donech <- err

				return err
			}, nil
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerPhaseDoneHeaderHint), handler,
			t.ci,
			newWorkerBrokerPhaseDoneFunc(33, "w3", 3, mergedOps, unmergedOps),
		)
		t.ErrorContains(err, "findme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case err = <-donech:
			t.ErrorContains(err, "findme")
		}
	})
}

func (t *testHandlers) TestWorkerSetStates() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sts := make([]base.State, 33)

		for i := range sts {
			sts[i] = t.randomState()
		}
		sort.Slice(sts, func(i, j int) bool {
			return cmp.Less[string](sts[i].Key(), sts[j].Key())
		})

		add, done, err := util.CompactAppendSliceV1[base.State](uint64(len(sts)), 1<<9)
		t.NoError(err)

		handler, err := newWorkerHandlerSetStatesFunc(func(st base.State) error {
			add(st)

			return nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSetStatesHeaderHint), handler,
			t.ci,
			newWorkerBrokerSetStatesFunc(func(f func(base.State) error) error {
				for i := range sts {
					if err := f(sts[i]); err != nil {
						return err
					}
				}

				return nil
			}),
		))

		done(func(s []base.State) error {
			t.Equal(len(sts), len(s))

			sort.Slice(s, func(i, j int) bool {
				return cmp.Less[string](s[i].Key(), s[j].Key())
			})

			for i := range sts {
				t.True(base.IsEqualState(sts[i], s[i]))
			}

			return nil
		})
	})

	t.Run("empty", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		add, done, err := util.CompactAppendSliceV1[base.State](1<<9, 1<<9)
		t.NoError(err)

		handler, err := newWorkerHandlerSetStatesFunc(func(st base.State) error {
			add(st)

			return nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSetStatesHeaderHint), handler,
			t.ci,
			newWorkerBrokerSetStatesFunc(func(f func(base.State) error) error {
				return nil
			}),
		))

		done(func(s []base.State) error {
			t.Empty(s)

			return nil
		})
	})
}

func (t *testHandlers) TestWorkerSetOperations() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		priv := base.NewDummyPrivatekey("showme")
		networkID := base.NetworkID(util.UUID().Bytes())

		ops := make([]base.Operation, 33)

		for i := range ops {
			fact := isaac.NewDummyOperationFact(util.UUID().Bytes(), valuehash.RandomSHA256())
			ops[i], _ = isaac.NewDummyOperation(fact, priv, networkID)
		}
		sort.Slice(ops, func(i, j int) bool {
			return cmp.Less[string](ops[i].Hash().String(), ops[j].Hash().String())
		})

		add, done, err := util.CompactAppendSliceV1[base.Operation](uint64(len(ops)), 1<<9)
		t.NoError(err)

		handler, err := newWorkerHandlerSetOperationsFunc(func(st base.Operation) error {
			add(st)

			return nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSetOperationsHeaderHint), handler,
			t.ci,
			newWorkerBrokerSetOperationsFunc(func(f func(base.Operation) error) error {
				for i := range ops {
					if err := f(ops[i]); err != nil {
						return err
					}
				}

				return nil
			}),
		))

		done(func(s []base.Operation) error {
			t.Equal(len(ops), len(s))

			sort.Slice(s, func(i, j int) bool {
				return cmp.Less[string](s[i].Hash().String(), s[j].Hash().String())
			})

			for i := range ops {
				base.EqualOperation(t.Assert(), ops[i], s[i])
			}

			return nil
		})
	})

	t.Run("empty", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		add, done, err := util.CompactAppendSliceV1[base.Operation](1<<9, 1<<9)
		t.NoError(err)

		handler, err := newWorkerHandlerSetOperationsFunc(func(op base.Operation) error {
			add(op)

			return nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSetOperationsHeaderHint), handler,
			t.ci,
			newWorkerBrokerSetOperationsFunc(func(f func(base.Operation) error) error {
				return nil
			}),
		))

		done(func(s []base.Operation) error {
			t.Empty(s)

			return nil
		})
	})
}

func (t *testHandlers) TestWorkerSyncStates() {
	t.Run("ok", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan base.Height, 1)
		handler, err := newWorkerHandlerSyncStatesFunc(func(height base.Height) error {
			donech <- height

			return nil
		})
		t.NoError(err)

		t.NoError(requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSyncStatesHeaderHint), handler,
			t.ci,
			newWorkerBrokerSyncStatesFunc(33),
		))

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case i := <-donech:
			t.Equal(base.Height(33), i)
		}
	})

	t.Run("error", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		donech := make(chan error, 1)
		handler, err := newWorkerHandlerSyncStatesFunc(func(base.Height) error {
			err := errors.Errorf("findme")
			donech <- err

			return err
		})
		t.NoError(err)

		err = requestFunc(ctx, t.Encs, handlerNameByHint(WorkerSyncStatesHeaderHint), handler,
			t.ci,
			newWorkerBrokerSyncStatesFunc(33),
		)
		t.ErrorContains(err, "findme")

		select {
		case <-time.After(time.Millisecond):
			t.Fail("not requested")
		case err = <-donech:
			t.ErrorContains(err, "findme")
		}
	})
}

func TestHandlers(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testHandlers))
}
