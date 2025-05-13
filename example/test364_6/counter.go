package dop6

import (
	"sync"
	"sync/atomic"
)

func newXMergeJobCounter(
	mergeDone func(),
	jobDone func(),
) (
	func(), /* count initial merge */
	func() (uint64, bool), /* initial merge count done */
	func(uint64) bool, /* count merge */
	func() bool, /* count merge end */
	func() bool, /* count initial unmerge */
) {
	var l sync.Mutex
	var initialMergeDone, initialUnmergeDone bool
	var im, cm, ne, bn, cn uint64

	checkMergeJobDone := func() bool {
		switch {
		case !initialMergeDone:
			return false
		case im < 1:
			return true
		}

		if im != cm {
			return false
		}

		bn = ne

		return bn == cn
	}

	check := func() bool {
		if !checkMergeJobDone() {
			return false
		}

		mergeDone()

		if !initialUnmergeDone {
			return false
		}

		jobDone()

		return true
	}

	return func() {
			l.Lock()
			defer l.Unlock()

			if initialMergeDone {
				return
			}

			im++
		},
		func() (uint64, bool) {
			l.Lock()
			defer l.Unlock()

			if initialMergeDone {
				return im, false
			}

			initialMergeDone = true

			return im, check()
		},
		func(next uint64) bool {
			l.Lock()
			defer l.Unlock()

			cm++
			ne += next

			return check()
		},
		func() bool {
			l.Lock()
			defer l.Unlock()

			cn++

			return check()
		},
		func() bool {
			l.Lock()
			defer l.Unlock()

			if initialUnmergeDone {
				return false
			}

			initialUnmergeDone = true

			return check()
		}
}

func newMergeJobCounter() (
	func(),
	func(uint64) bool,
	func() bool,
	func() (uint64, bool),
) {
	var l sync.Mutex
	var initialDone bool
	var in, b0, c0, b1, c1, c2 uint64

	check := func() bool {
		switch {
		case !initialDone:
			return false
		case b0 < 1:
			return true
		case c0 != b0:
			return false
		}

		b1 = c1

		switch {
		case b1 < 1, c2 == b1:
			return true
		default:
			return false
		}
	}

	return func() {
			l.Lock()
			defer l.Unlock()

			if initialDone {
				return
			}

			in++
		},
		func(next uint64) bool {
			l.Lock()
			defer l.Unlock()

			c0++
			c1 += next

			return check()
		},
		func() bool {
			l.Lock()
			defer l.Unlock()

			c2++

			return check()
		},
		func() (uint64, bool) {
			l.Lock()
			defer l.Unlock()

			if initialDone {
				return b0, false
			}

			b0 = in
			initialDone = true

			return b0, check()
		}
}

func newPhaseJobCounter() (
	func(),
	func(uint8, uint64) bool,
	func() (uint64, bool),
	func(),
) {
	var l sync.Mutex
	var p0 uint64
	var pc, pn map[uint8]uint64

	var nextChecked uint8
	var check func(phase uint8) bool

	check = func(phase uint8) bool {
		if phase < nextChecked {
			return check(nextChecked)
		}

		switch i, found := pc[phase]; {
		case !found:
			return false
		case i < 1:
			return true
		}

		if pn[phase] < pc[phase] {
			return false
		}

		nextChecked = phase + 1

		return check(nextChecked)
	}

	return func() {
			atomic.AddUint64(&p0, 1)
		},
		func(phase uint8, next uint64) bool {
			l.Lock()
			defer l.Unlock()

			if pc == nil {
				pc = map[uint8]uint64{}
			}

			if pn == nil {
				pn = map[uint8]uint64{}
			}

			pn[phase]++
			pc[phase+1] += next

			return check(nextChecked)
		},
		func() (uint64, bool) {
			l.Lock()
			defer l.Unlock()

			switch pc {
			case nil:
				pc = map[uint8]uint64{}
			default:
				if i, found := pc[0]; found {
					return i, false
				}
			}

			c0 := atomic.LoadUint64(&p0)
			pc[0] = c0

			if c0 < 1 {
				return 0, true
			}

			return c0, check(nextChecked)
		},
		func() {
			l.Lock()
			defer l.Unlock()

			clear(pc)
			clear(pn)
		}
}
