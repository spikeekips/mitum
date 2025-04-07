package util

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type testUnlimitedChans struct {
	BaseSuite
}

func (t *testUnlimitedChans) TestInsert() {
	t.RunWithGoLeak("insert", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 0, 0)

		var expected []int
		for i := range 333 {
			t.NoError(insert(i))

			expected = append(expected, i)
		}

		done()
		collected := slices.Collect(seq)
		t.T().Log("collected:", collected)

		t.Equal(expected, collected)
	})

	t.RunWithGoLeak("insert; seq delayed", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var expected, collected []int

		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
				<-time.After(time.Millisecond * 5)
			}

			close(seqdone)
		}()

		insertdone := make(chan struct{})
		go func() {
			for i := range 333 {
				t.IfErrPanic(insert(i))

				expected = append(expected, i)
			}

			done()
			close(insertdone)
		}()

		select {
		case <-time.After(time.Millisecond * 33):
			t.Fail("wait insert")
		case <-insertdone:
		}

		select {
		case <-time.After(time.Second * 3):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.Equal(expected, collected)
		}
	})

	t.RunWithGoLeak("insert; seq", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var expected, collected []int

		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
			}

			close(seqdone)
		}()

		go func() {
			for i := range 333 {
				t.IfErrPanic(insert(i))

				expected = append(expected, i)
			}

			done()
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.Equal(expected, collected)
		}
	})

	t.RunWithGoLeak("insert: async", func() {
		for range 333 {
			ctx, cancel := t.Ctx()
			defer cancel(nil)

			insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

			var expected, collected []int

			seqdone := make(chan struct{})
			go func() {
				for i := range seq {
					collected = append(collected, i)
				}

				close(seqdone)
			}()

			go func() {
				var wg sync.WaitGroup

				for i := range 333 {
					expected = append(expected, i)

					wg.Add(1)

					go func() {
						t.IfErrPanic(insert(i))

						wg.Done()
					}()
				}

				wg.Wait()

				done()
			}()

			select {
			case <-time.After(time.Second):
				t.Fail("wait seq")
			case <-seqdone:
				slices.Sort(collected)
				t.Equal(expected, collected)
			}
		}
	})
}

func (t *testUnlimitedChans) TestInsertDone() {
	t.RunWithGoLeak("done during insert", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
			}

			close(seqdone)
		}()

		insertdone := make(chan struct{})

		go func() {
			var wg sync.WaitGroup
			var count int64

			for i := range 333 {
				wg.Add(1)

				go func() {
					_ = insert(i)

					if atomic.AddInt64(&count, 1) > 99 {
						done()
					}

					wg.Done()
				}()
			}

			wg.Wait()
			close(insertdone)
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.NotEmpty(collected)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait insert")
		case <-insertdone:
		}
	})
}

func (t *testUnlimitedChans) TestSeq() {
	t.RunWithGoLeak("empty insert", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		_, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
			}

			close(seqdone)
		}()

		done()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.Nil(collected)
		}
	})

	t.RunWithGoLeak("stop during iter", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		seqdone := make(chan struct{})
		go func() {
			for count, i := range SeqAll(seq) {
				if count == 133 {
					break
				}

				collected = append(collected, i)
			}

			close(seqdone)
		}()

		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				wg.Add(1)

				go func() {
					_ = insert(i)

					wg.Done()
				}()
			}

			wg.Wait()
			done()
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.True(len(collected) >= 133)
		}
	})

	t.RunWithGoLeak("no iter", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, _ := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var wg sync.WaitGroup

		for i := range 333 {
			wg.Add(1)

			go func() {
				_ = insert(i)

				wg.Done()
			}()
		}

		wg.Wait()

		done()
	})

	t.RunWithGoLeak("iter; nil yield", func() {
		ctx, cancel := t.Ctx()
		defer cancel(nil)

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		seqdone := make(chan struct{})
		go func() {
			seq(nil)
			close(seqdone)
		}()

		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				wg.Add(1)

				go func() {
					_ = insert(i)

					wg.Done()
				}()
			}

			wg.Wait()

			done()
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
		}
	})
}

func (t *testUnlimitedChans) TestContext() {
	t.RunWithGoLeak("cancel before new", func() {
		ctx, cancel := t.Ctx()
		cancel(errors.Errorf("showme"))

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)
		err := insert(1)
		t.ErrorIs(err, context.Canceled)
		done()

		var collected []int
		for i := range seq {
			collected = append(collected, i)
		}
		t.Nil(collected)
	})

	t.RunWithGoLeak("cancel during insert", func() {
		ctx, cancel := t.Ctx()

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		var onceCollected int64
		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
				atomic.CompareAndSwapInt64(&onceCollected, 0, 1)
			}

			close(seqdone)
		}()

		insertdone := make(chan struct{})
		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				wg.Add(1)

				go func() {
					if i%33 == 0 && atomic.LoadInt64(&onceCollected) > 0 {
						cancel(errors.Errorf("showme"))
					}

					_ = insert(i)

					wg.Done()
				}()
			}

			wg.Wait()
			done()

			close(insertdone)
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.NotEmpty(collected)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait insert")
		case <-insertdone:
		}
	})

	t.RunWithGoLeak("cancel before done", func() {
		ctx, cancel := t.Ctx()

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var expected, collected []int

		seqdone := make(chan struct{})
		go func() {
			for i := range seq {
				collected = append(collected, i)
			}

			close(seqdone)
		}()

		var canceled int64
		insertdone := make(chan struct{})
		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				expected = append(expected, i)

				wg.Add(1)

				go func() {
					if err := insert(i); errors.Is(err, context.Canceled) {
						atomic.AddInt64(&canceled, 1)
					}

					wg.Done()
				}()
			}

			wg.Wait()

			cancel(errors.Errorf("showme"))
			done()

			close(insertdone)
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			slices.Sort(collected)
			t.Equal(expected, collected)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait insert")
		case <-insertdone:
			t.True(canceled < 1)
		}
	})

	t.RunWithGoLeak("cancel before seq", func() {
		ctx, cancel := t.Ctx()

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		seqdone := make(chan struct{})
		go func() {
			for count, i := range SeqAll(seq) {
				collected = append(collected, i)
				if count == 133 {
					cancel(errors.Errorf("showme"))
				}
			}

			close(seqdone)
		}()

		insertdone := make(chan struct{})
		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				wg.Add(1)

				go func() {
					_ = insert(i)

					wg.Done()
				}()
			}

			wg.Wait()

			done()

			close(insertdone)
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.NotEmpty(collected)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait insert")
		case <-insertdone:
		}
	})

	t.RunWithGoLeak("cancel before delayed seq", func() {
		ctx, cancel := t.Ctx()

		insert, done, seq := NewUnlimitedChans[int](ctx, 1<<9, 1<<9)

		var collected []int

		seqdone := make(chan struct{})
		go func() {
			defer close(seqdone)

			for i := range seq {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Minute):
					collected = append(collected, i)
				}
			}
		}()

		insertdone := make(chan struct{})
		go func() {
			var wg sync.WaitGroup

			for i := range 333 {
				wg.Add(1)

				go func() {
					_ = insert(i)

					wg.Done()
				}()
			}

			wg.Wait()

			done()

			close(insertdone)
		}()

		select {
		case <-time.After(time.Second):
			t.Fail("wait insert")
		case <-insertdone:
			cancel(nil)
		}

		select {
		case <-time.After(time.Second):
			t.Fail("wait seq")
		case <-seqdone:
			t.T().Log("collected:", collected)
			t.Empty(collected)
		}
	})
}

func TestUnlimitedChans(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testUnlimitedChans))
}
