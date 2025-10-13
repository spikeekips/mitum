package main

import (
	"cmp"
	"context"
	"fmt"
	"iter"
	"os"
	"slices"
	"strconv"

	"golang.org/x/sync/semaphore"
	"gonum.org/v1/gonum/stat/combin"
)

func sum(i []int) int {
	var s int
	for i := range slices.Values(i) {
		s += i
	}

	return s
}

func asw(nStates, left int, l []int) iter.Seq[[]int] {
	return func(yield func([]int) bool) {
		for i := range nStates {
			n := i + 1

			if left < n {
				break
			}

			if j := len(l); j > 0 && n < l[j-1] {
				continue
			}

			ll := append(l, n)

			for c := range asw(nStates, left-n, append([]int(nil), ll...)) {
				if !yield(c) {
					return
				}
			}

			if left <= n {
				_ = yield(ll)

				return
			}

		}
	}
}

func isValidAssigned(nStates, share int, c []int) (int, bool) {
	total := nStates * share

	var s int

	for i := range slices.Values(c) {
		s += i
	}

	return s, s == total
}

func compareC(a, b []int) int {
	switch {
	case a == nil, b == nil:
		return 0
	case a == nil:
		return 1
	case b == nil:
		return -1
	case len(a) != len(b):
		return cmp.Compare(len(a), len(b))
	default:
		return slices.Compare(a, b)
	}
}

func fi[T any](c []int, o []T) []T {
	d := make([]T, len(c))

	for i, v := range slices.All(c) {
		d[i] = o[v]
	}

	return d
}

func newLeft(c, left []int) []int {
	l := append([]int(nil), left...)
	if len(c) < 1 {
		return l
	}

	for i := range slices.Values(c) {
		idx := slices.Index(l, i)

		l[idx] = l[len(l)-1]
		l = l[:len(l)-1]
	}

	slices.Sort(l)

	return l
}

func cf(n int, left []int) iter.Seq2[[]int, []int] {
	l := append([]int(nil), left...)
	slices.Sort(l)
	s := slices.Compact(l)

	return func(yield func([]int, []int) bool) {
		if len(s) < n {
			return
		}

		g := combin.NewCombinationGenerator(len(s), n)

		for g.Next() {
			c := g.Combination(nil)
			v := fi(c, s)

			if !yield(v, newLeft(v, left)) {
				return
			}
		}
	}
}

func cfy(c, left []int, y [][]int) iter.Seq[[][]int] {
	return func(yield func([][]int) bool) {
		n := c[0]

		for i, j := range cf(n, left) {
			if len(y) > 0 && compareC(i, y[len(y)-1]) < 0 {
				continue
			}

			y0 := append([][]int(nil), y...)
			y0 = append(y0, i)

			if len(c[1:]) < 1 {
				if !yield(y0) {
					return
				}

				continue
			}

			for k := range cfy(c[1:], j, y0) {
				if !yield(k) {
					return
				}
			}
		}
	}
}

func cfs(c, total []int) iter.Seq[[][]int] {
	return cfy(c, append([]int(nil), total...), nil)
}

func acfs(nStates, share int) iter.Seq2[[]int, [][]int] {
	return func(yield func([]int, [][]int) bool) {
		nLeft := nStates * share

		states := make([]int, nStates)
		total := make([]int, nStates*share)
		for i := range states {
			states[i] = i
			for j := range share {
				total[(i*share)+j] = i
			}
		}

		var l []int

		for c := range asw(nStates, nLeft, l) {
			if s, ok := isValidAssigned(nStates, share, c); !ok {
				fmt.Printf("invalid assigned: %d, %v\n", s, c)

				return
			}

			for i := range cfs(c, total) {
				if !yield(c, i) {
					return
				}
			}
		}
	}
}

func selectFail(y [][]int, fail int) iter.Seq2[[]int, [][]int] {
	return func(yield func([]int, [][]int) bool) {
		olds := map[string]struct{}{}
		defer clear(olds)

		if len(y) < fail {
			return
		}

		g := combin.NewCombinationGenerator(len(y), fail)
		for g.Next() {
			c := g.Combination(nil)
			v := fi(c, y)

			k := fmt.Sprintf("%v", v)
			switch _, found := olds[k]; {
			case found:
				continue
			default:
				olds[k] = struct{}{}
			}

			if !yield(c, v) {
				return
			}
		}
	}
}

func checkFailed(share int, f [][]int) bool {
	c := map[int]int{}

	for i := range slices.Values(f) {
		for j := range slices.Values(i) {
			c[j]++
			if c[j] >= share {
				return true
			}
		}
	}

	return false
}

type job struct {
	a  [][]int
	c  []int
	f  [][]int
	ca []int
}

func sf(nStates, share, fail int) {
	for c, a := range acfs(nStates, share) {
		for ca, f := range selectFail(a, fail) {
			isFailed := checkFailed(share, f)
			sx := "o"
			if isFailed {
				sx = "x"
			}

			b := fi(ca, c)
			fmt.Println(sx, sum(b), b, "|", f, "|", a)
		}
	}
}

func jsf(nStates, share, fail int, ch chan func()) {
	for c, a := range acfs(nStates, share) {
		for ca, f := range selectFail(a, fail) {
			ch <- func() {
				jsfc(share, job{a: a, c: c, f: f, ca: ca})
			}
		}
	}

	close(ch)
}

func jsfc(share int, j job) {
	isFailed := checkFailed(share, j.f)
	sx := "o"
	if isFailed {
		sx = "x"
	}

	b := fi(j.ca, j.c)
	fmt.Println(sx, sum(b), b, "|", j.f, "|", j.a)
}

func newJP() (chan func(), func()) {
	ctx := context.Background()
	sem := semaphore.NewWeighted(99)

	ch := make(chan func())
	done := make(chan struct{})

	go func() {
		for f := range ch {
			go func() {
				if err := sem.Acquire(ctx, 1); err != nil {
					panic(err)

					return
				}
				defer sem.Release(1)

				f()
			}()
		}

		close(done)
	}()

	return ch, func() {
		<-done
	}
}

func usage(msg string, err error) {
	var errs string

	if msg != "" {
		errs = fmt.Sprintf(": %s", msg)
	}

	if err != nil {
		errs = fmt.Sprintf("%s: %+v", msg, err)
	}

	fmt.Fprintf(os.Stderr, "%s <number of states> <share> <fail>\n%s\n", os.Args[0], errs)

	os.Exit(1)
}

func main() {
	if len(os.Args) < 4 {
		usage("", nil)
	}

	var nStates, share, fail int

	switch i, err := strconv.ParseInt(os.Args[1], 10, 64); {
	case err != nil:
		usage("invalid <number of states>", err)
	default:
		nStates = int(i)
	}

	switch i, err := strconv.ParseInt(os.Args[2], 10, 64); {
	case err != nil:
		usage("invalid <share>", err)
	default:
		share = int(i)
	}

	switch i, err := strconv.ParseInt(os.Args[3], 10, 64); {
	case err != nil:
		usage("invalid <fail>", err)
	default:
		fail = int(i)
	}

	switch {
	case fail > nStates*share:
		usage("<fail> too big; <fail> <= <number of states> * <share>", nil)
	}

	sf(nStates, share, fail)
	return

	ch, done := newJP()
	jsf(nStates, share, fail, ch)

	done()
}
