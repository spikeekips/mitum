package util

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
)

type testTimers struct {
	suite.Suite
}

func (t *testTimers) timer(id TimerID) *ContextTimer {
	timer := NewContextTimer(
		id,
		time.Second*10,
		func(context.Context, int) (bool, error) {
			return true, nil
		},
	)

	return timer
}

func (t *testTimers) TestStart() {
	ids := []TimerID{
		"showme",
	}

	timers := NewTimers(ids, false)
	t.NoError(timers.Start(context.Background()))
}

func (t *testTimers) TestAllowNew() {
	ids := []TimerID{
		"showme",
		"findme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	id := TimerID("showme")
	t.NoError(timers.SetTimer(t.timer(id)))

	unknown := TimerID("unknown")
	t.Error(timers.SetTimer(t.timer(unknown)))
}

func (t *testTimers) TestStartTimer() {
	ids := []TimerID{
		"showme",
		"findme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	startID := TimerID("showme")
	stoppedID := TimerID("findme")

	t.NoError(timers.StartTimers([]TimerID{startID}, true))

	t.True(timers.timers[startID].IsStarted())
	t.Nil(timers.timers[stoppedID])
	t.True(timers.IsTimerStarted(startID))
}

func (t *testTimers) TestStartTimerStopOthers() {
	ids := []TimerID{
		"showme",
		"findme",
		"eatme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	// start all
	t.NoError(timers.StartTimers(ids, true))

	// start again only one
	startID := TimerID("showme")
	t.NoError(timers.StartTimers([]TimerID{startID}, true))
	t.True(timers.IsTimerStarted(startID))

	for _, id := range ids {
		if id == startID {
			continue
		}
		t.Nil(timers.timers[id])
		t.False(timers.IsTimerStarted(id))
	}
}

func (t *testTimers) TestStartTimerNotStop() {
	ids := []TimerID{
		"showme",
		"findme",
		"eatme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	// start all except startID
	t.NoError(timers.StartTimers(ids, true))

	startID := TimerID("showme")
	t.NoError(timers.StopTimers([]TimerID{startID}))
	t.Nil(timers.timers[startID])

	t.NoError(timers.SetTimer(t.timer(startID)))
	t.NoError(timers.StartTimers([]TimerID{startID}, false))

	for _, id := range ids {
		t.True(timers.timers[id].IsStarted())
	}
}

func (t *testTimers) TestStopTimer() {
	ids := []TimerID{
		"showme",
		"findme",
		"eatme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	// start all
	t.NoError(timers.StartTimers(ids, true))

	for _, id := range ids {
		t.True(timers.timers[id].IsStarted())
	}

	stopID := TimerID("eatme")
	t.NoError(timers.StopTimers([]TimerID{stopID}))
	t.Nil(timers.timers[stopID])

	for _, id := range ids {
		if id == stopID {
			continue
		}

		t.True(timers.timers[id].IsStarted())
	}

	st := timers.Started()
	t.Equal(2, len(st))

	started := make([]string, len(timers.Started()))
	for i := range st {
		started[i] = st[i].String()
	}

	t.True(InSlice(started, "showme") >= 0)
	t.True(InSlice(started, "findme") >= 0)
}

func (t *testTimers) TestStopTimersAll() {
	ids := []TimerID{
		"showme",
		"findme",
		"eatme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	// start all
	t.NoError(timers.StartTimers(ids, true))

	for _, id := range ids {
		t.True(timers.timers[id].IsStarted())
	}

	t.NoError(timers.StopTimersAll())

	for _, id := range ids {
		t.Nil(timers.timers[id])
	}
}

func (t *testTimers) TestStop() {
	ids := []TimerID{
		"showme",
		"findme",
		"eatme",
	}

	timers := NewTimers(ids, false)
	defer func() {
		_ = timers.Stop()
	}()

	for _, id := range ids {
		t.NoError(timers.SetTimer(t.timer(id)))
	}

	// start all
	t.NoError(timers.StartTimers(ids, true))

	for _, id := range ids {
		t.True(timers.timers[id].IsStarted())
	}

	t.NoError(timers.Stop())

	for _, id := range ids {
		t.Nil(timers.timers[id])
	}
}

func TestTimers(t *testing.T) {
	suite.Run(t, new(testTimers))
}

type testSimpleTimers struct {
	suite.Suite
}

func (t *testSimpleTimers) TestNew() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	called := make(chan string, 4)
	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			if i == 3 {
				go func() {
					<-time.After(time.Millisecond * 333)
					closech <- struct{}{}
				}()

				return 0
			}

			return time.Millisecond * 3
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("a-%d", i)

			return true, nil
		},
		nil,
	))

	var received []string

end:
	for {
		select {
		case <-closech:
			break end
		case i := <-called:
			t.T().Log("called:", i)

			received = append(received, i)
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2"}, received)
}

func (t *testSimpleTimers) TestVariableInterval() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	interval := time.Millisecond * 33

	closech := make(chan struct{}, 1)
	called := make(chan [2]interface{}, 4)

	started := time.Now()

	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			if i == 3 {
				go func() {
					<-time.After(time.Millisecond * 333)
					closech <- struct{}{}
				}()

				return 0
			}

			it := interval * time.Duration(i+1)

			return it
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- [2]interface{}{
				i,
				time.Now(),
			}

			return true, nil
		},
		nil,
	))

	var rids []uint64
	var rnows []time.Time

end:
	for {
		select {
		case <-closech:
			break end
		case i := <-called:
			rid := i[0].(uint64)
			rnow := i[1].(time.Time)

			rids = append(rids, rid)
			rnows = append(rnows, rnow)
		}
	}

	t.Equal([]uint64{0, 1, 2}, rids)

	prev := started
	for i := range rnows {
		now := rnows[i]

		sub := now.Sub(prev)

		base := interval * time.Duration(i+1)
		diff := interval

		t.T().Log("received interval:", i, sub, base, base-diff, base+diff)

		t.True(sub > base-diff)
		t.True(sub < base+diff)

		prev = now
	}
}

func (t *testSimpleTimers) TestMultiple() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	called := make(chan [2]interface{}, 10)

	ainterval := time.Millisecond * 30
	astarted := time.Now()
	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			if i == 3 {
				return 0
			}

			return ainterval
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- [2]interface{}{
				fmt.Sprintf("a-%d", i),
				time.Now(),
			}

			return true, nil
		},
		nil,
	))

	bstarted := time.Now()

	binterval := time.Millisecond * 60
	ts.NewTimer(NewSimpleTimer(
		TimerID("b"),
		func(i uint64) time.Duration {
			if i == 3 {
				go func() {
					<-time.After(time.Millisecond * 333)
					closech <- struct{}{}
				}()

				return 0
			}

			return binterval
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- [2]interface{}{
				fmt.Sprintf("b-%d", i),
				time.Now(),
			}

			return true, nil
		},
		nil,
	))

	for range make([]int, 33) {
		ts.NewTimer(NewSimpleTimer(
			TimerID(UUID().String()),
			func(i uint64) time.Duration {
				return time.Millisecond * 11
			},
			func(_ context.Context, i uint64) (bool, error) {
				called <- [2]interface{}{
					fmt.Sprintf("c-%d", i),
					time.Now(),
				}

				return true, nil
			},
			nil,
		))
	}

	var aids, bids []string
	var anows, bnows []time.Time

end:
	for {
		select {
		case <-closech:
			break end
		case i := <-called:
			id := i[0].(string)
			now := i[1].(time.Time)

			if strings.HasPrefix(id, "a-") {
				aids = append(aids, id)
				anows = append(anows, now)
			}

			if strings.HasPrefix(id, "b-") {
				bids = append(bids, id)
				bnows = append(bnows, now)
			}
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2"}, aids)
	t.Equal([]string{"b-0", "b-1", "b-2"}, bids)

	aprev := astarted
	for i := range anows {
		now := anows[i]

		sub := now.Sub(aprev)

		diff := ainterval

		t.T().Log("received interval:", i, sub, ainterval, ainterval-diff, ainterval+diff)

		t.True(sub > ainterval-diff)
		t.True(sub < ainterval+diff)

		aprev = now
	}

	bprev := bstarted
	for i := range bnows {
		now := bnows[i]

		sub := now.Sub(bprev)

		diff := binterval

		t.T().Log("received interval:", i, sub, binterval, binterval-diff, binterval+diff)

		t.True(sub > binterval-diff)
		t.True(sub < binterval+diff)

		bprev = now
	}
}

func (t *testSimpleTimers) TestStopTimers() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	removech := make(chan struct{})

	called := make(chan string, 4)
	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			return time.Millisecond * 33
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("a-%d", i)

			if i == 2 {
				go func() {
					removech <- struct{}{}
				}()
			}

			return true, nil
		},
		nil,
	))

	go func() {
		<-removech

		_ = ts.StopTimers([]TimerID{"a"})

		<-time.After(time.Millisecond * 333)
		closech <- struct{}{}
	}()

	var received []string

end:
	for {
		select {
		case <-closech:
			break end
		case i, notclosed := <-called:
			if !notclosed {
				break end
			}

			t.T().Log("called:", i)

			received = append(received, i)
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2"}, received)
}

func (t *testSimpleTimers) TestStop() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	removech := make(chan struct{})

	called := make(chan string, 4)
	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			return time.Millisecond * 33
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("a-%d", i)

			if i == 2 {
				go func() {
					removech <- struct{}{}
				}()
			}

			return true, nil
		},
		nil,
	))

	go func() {
		<-removech

		_ = ts.Stop()

		<-time.After(time.Millisecond * 333)
		closech <- struct{}{}
	}()

	var received []string

end:
	for {
		select {
		case <-closech:
			break end
		case i, notclosed := <-called:
			if !notclosed {
				break end
			}

			t.T().Log("called:", i)

			received = append(received, i)
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2"}, received)
}

func (t *testSimpleTimers) TestMultipleStop() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	removech := make(chan struct{})
	called := make(chan string, 10)

	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			return time.Millisecond * 33
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("a-%d", i)

			return true, nil
		},
		nil,
	))

	ts.NewTimer(NewSimpleTimer(
		TimerID("b"),
		func(i uint64) time.Duration {
			return time.Millisecond * 66
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("b-%d", i)

			if i > 3 {
				go func() {
					removech <- struct{}{}
				}()
			}

			return true, nil
		},
		nil,
	))

	go func() {
		<-removech

		_ = ts.Stop()

		<-time.After(time.Millisecond * 333)
		closech <- struct{}{}
	}()

end:
	for {
		select {
		case <-closech:
			break end
		case _, notclosed := <-called:
			if !notclosed {
				break end
			}
		}
	}
}

func (t *testSimpleTimers) TestStopOther() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	called := make(chan string, 10)

	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			if i == 5 {
				return 0
			}

			return time.Millisecond * 33
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("a-%d", i)

			if i == 2 {
				ts.StopTimers([]TimerID{"b"})
			}

			return true, nil
		},
		nil,
	))

	ts.NewTimer(NewSimpleTimer(
		TimerID("b"),
		func(i uint64) time.Duration {
			return time.Millisecond * 33
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- fmt.Sprintf("b-%d", i)

			return true, nil
		},
		nil,
	))

	go func() {
		<-time.After(time.Second)
		closech <- struct{}{}
	}()

	var aids, bids []string

	closecalled := time.After(time.Second * 2)
	var closeonce sync.Once
end:
	for {
		select {
		case <-closech:
			break end
		case <-closecalled:
			closeonce.Do(func() {
				go func() {
					_ = ts.Stop()

					<-time.After(time.Millisecond * 333)
					closech <- struct{}{}
				}()
			})
		case i, notclosed := <-called:
			if !notclosed {
				break end
			}

			if strings.HasPrefix(i, "a-") {
				aids = append(aids, i)
			} else {
				bids = append(bids, i)
			}
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2", "a-3", "a-4"}, aids)
	t.True(len(bids) < 5)
}

func (t *testSimpleTimers) TestLongRunning() {
	ts, _ := NewSimpleTimers(1, time.Millisecond)

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	closech := make(chan struct{}, 1)
	called := make(chan [2]interface{}, 10)

	ainterval := time.Millisecond * 30
	astarted := time.Now()
	ts.NewTimer(NewSimpleTimer(
		TimerID("a"),
		func(i uint64) time.Duration {
			if i == 3 {
				return 0
			}

			return ainterval
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- [2]interface{}{
				fmt.Sprintf("a-%d", i),
				time.Now(),
			}

			if i == 1 {
				<-time.After(time.Millisecond * 333)
			}

			return true, nil
		},
		nil,
	))

	bstarted := time.Now()

	binterval := time.Millisecond * 30
	ts.NewTimer(NewSimpleTimer(
		TimerID("b"),
		func(i uint64) time.Duration {
			return binterval
		},
		func(_ context.Context, i uint64) (bool, error) {
			called <- [2]interface{}{
				fmt.Sprintf("b-%d", i),
				time.Now(),
			}

			return true, nil
		},
		nil,
	))

	var aids, bids []string
	var anows, bnows []time.Time

	closecalled := time.After(time.Second * 2)

	var closeonce sync.Once
end:
	for {
		select {
		case <-closech:
			break end
		case <-closecalled:
			closeonce.Do(func() {
				go func() {
					_ = ts.Stop()

					<-time.After(time.Millisecond * 333)
					closech <- struct{}{}
				}()
			})
		case i, notclosed := <-called:
			if !notclosed {
				break end
			}

			id := i[0].(string)
			now := i[1].(time.Time)

			if strings.HasPrefix(id, "a-") {
				aids = append(aids, id)
				anows = append(anows, now)
			} else {
				bids = append(bids, id)
				bnows = append(bnows, now)
			}
		}
	}

	t.Equal([]string{"a-0", "a-1", "a-2"}, aids)
	t.True(len(bids) > 10)

	aprev := astarted
	for i := range anows {
		now := anows[i]

		sub := now.Sub(aprev)

		diff := ainterval
		if i == 2 {
			diff = time.Millisecond*333 + ainterval
		}

		t.T().Log("received a interval:", i, sub, ainterval, ainterval-diff, ainterval+diff)

		t.True(sub > ainterval-diff)
		t.True(sub < ainterval+diff)

		aprev = now
	}

	bprev := bstarted
	for i := range bnows {
		now := bnows[i]

		sub := now.Sub(bprev)

		diff := binterval

		t.T().Log("received a interval:", i, sub, binterval, binterval-diff, binterval+diff)

		t.True(sub > ainterval-diff)
		t.True(sub < ainterval+diff)

		bprev = now
	}
}

func (t *testSimpleTimers) TestUnknownID() {
	ts, _ := NewSimpleTimersFixedIDs(1, time.Millisecond, []TimerID{"a"})

	t.NoError(ts.Start(context.Background()))
	defer ts.Stop()

	t.Run("add ok", func() {
		added, err := ts.NewTimer(NewSimpleTimer(
			TimerID("a"),
			func(i uint64) time.Duration {
				return time.Millisecond * 33
			},
			func(_ context.Context, i uint64) (bool, error) {
				return true, nil
			},
			nil,
		))

		t.True(added)
		t.NoError(err)
	})

	t.Run("add again", func() {
		added, err := ts.NewTimer(NewSimpleTimer(
			TimerID("a"),
			func(i uint64) time.Duration {
				return time.Millisecond * 33
			},
			func(_ context.Context, i uint64) (bool, error) {
				return true, nil
			},
			nil,
		))

		t.True(added)
		t.NoError(err)
	})

	t.Run("unknown", func() {
		added, err := ts.NewTimer(NewSimpleTimer(
			TimerID("b"),
			func(i uint64) time.Duration {
				return time.Millisecond * 33
			},
			func(_ context.Context, i uint64) (bool, error) {
				return true, nil
			},
			nil,
		))

		t.False(added)
		t.Error(err)
		t.ErrorContains(err, "unknown timer")
	})
}

func TestSimpleTimers(t *testing.T) {
	suite.Run(t, new(testSimpleTimers))
}
