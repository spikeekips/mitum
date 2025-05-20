package dop6

import (
	"iter"
	"math"
)

type opsWorkersItemsSeq = iter.Seq2[string /* worker */, func() (uint64, iter.Seq[uint64 /* opindex */])]

func newOpsWorkersNItems(workers, share, items uint64) (
	uint64,
	iter.Seq2[uint64 /* worker */, uint64 /* number of items */],
) {
	if workers < 1 || share < 1 || items < 1 {
		return 0, func(func(uint64, uint64) bool) {}
	}

	share = min(share, workers)

	total := items * share

	ni := uint64(math.Floor(float64(total) / float64(workers)))
	wl := total - (ni * workers)

	ow := wl
	if ni > 0 {
		ow = workers
	}

	return ow, func(yield func(uint64, uint64) bool) {
		for w := range ow {
			var n uint64

			switch {
			case w >= wl:
				n = ni
			default:
				n = ni + 1
			}

			if !yield(w, n) {
				return
			}
		}
	}
}

func newOpsWorkersItems(workers []string, share, items uint64) (
	[]string,
	opsWorkersItemsSeq,
) {
	if len(workers) < 1 || share < 1 || items < 1 {
		return nil, func(func(string, func() (uint64, iter.Seq[uint64])) bool) {}
	}

	nworkers := uint64(len(workers))

	share = min(share, nworkers)

	pos := func(worker, item uint64) uint64 {
		return uint64(math.Floor(
			float64((nworkers*item)+worker) / float64(share),
		))
	}

	ow, seq := newOpsWorkersNItems(nworkers, share, items)

	return workers[:ow], func(yield func(string, func() (uint64, iter.Seq[uint64])) bool) {
		for nw, n := range seq {
			if !yield(
				workers[nw],
				func() (uint64, iter.Seq[uint64]) {
					return n, func(yield2 func(uint64) bool) {
						for i := range n {
							if !yield2(pos(nw, i)) {
								return
							}
						}
					}
				},
			) {
				return
			}
		}
	}
}
