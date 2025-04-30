package m2

import (
	"fmt"
	"os"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
	"gonum.org/v1/gonum/stat/combin"
)

type testXMergeJobCounter struct {
	suite.Suite
}

func (t *testXMergeJobCounter) funcsJobDone() (*int64, *int64, func(), func()) {
	var l, j int64

	return &l, &j,
		func() {
			atomic.AddInt64(&l, 1)
		},
		func() {
			atomic.AddInt64(&j, 1)
		}
}

func (t *testXMergeJobCounter) TestJobDone() {
	t.Run("done after 0 count", func() {
		im, dm, _, _, _, _, _ := newXMergeJobCounter(func() {}, func() {})
		im()
		im()
		im()

		rm, isDone := dm()
		t.EqualValues(3, rm)
		t.False(isDone)
	})

	t.Run("0 count after done", func() {
		im, dm, _, _, _, _, _ := newXMergeJobCounter(func() {}, func() {})

		im()
		im()

		rm, isDone := dm()
		t.EqualValues(2, rm)
		t.False(isDone)

		im()

		rm, isDone = dm()
		t.EqualValues(2, rm)
		t.False(isDone)
	})

	t.Run("done before 0 count", func() {
		im, dm, _, _, _, du, _ := newXMergeJobCounter(func() {}, func() {})

		rm, isDone := dm()
		t.EqualValues(0, rm)
		t.False(isDone)

		cu, isDone := du()
		t.EqualValues(0, cu)
		t.True(isDone)

		im()

		rm, isDone = dm()
		t.EqualValues(0, rm)
		t.False(isDone)

		cu, isDone = du()
		t.EqualValues(0, cu)
		t.False(isDone)
	})
}

func (t *testXMergeJobCounter) TestCheck() {
	t.Run("no next, no unmerge", func() {
		t.Run("merge done true after count", func() {
			l, j, mergeJobDone, jobDone := t.funcsJobDone()

			im, dm, cm, _, _, _, _ := newXMergeJobCounter(mergeJobDone, jobDone)

			im()
			im()
			im()

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(0))

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			t.EqualValues(1, *l)
			t.EqualValues(0, *j)
		})

		t.Run("done true after count", func() {
			l, j, mergeJobDone, jobDone := t.funcsJobDone()

			im, dm, cm, _, _, du, _ := newXMergeJobCounter(mergeJobDone, jobDone)

			im()
			im()
			im()

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(0))

			ru, isDone := du()
			t.False(isDone)
			t.EqualValues(0, ru)

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.True(isDone)

			t.EqualValues(1, *l)
			t.EqualValues(1, *j)
		})

		t.Run("merge done false with count", func() {
			l, j, mergeJobDone, jobDone := t.funcsJobDone()

			im, dm, cm, _, _, du, _ := newXMergeJobCounter(mergeJobDone, jobDone)

			im()
			im()
			im()

			t.False(cm(0))
			t.False(cm(0))

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.False(isDone)

			t.True(cm(0))

			t.EqualValues(1, *l)
			t.EqualValues(1, *j)
		})

		t.Run("done false before count", func() {
			l, j, mergeJobDone, jobDone := t.funcsJobDone()

			im, dm, cm, _, _, du, _ := newXMergeJobCounter(mergeJobDone, jobDone)

			im()
			im()
			im()

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.False(isDone)

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.True(cm(0))

			t.EqualValues(1, *l)
			t.EqualValues(1, *j)
		})
	})

	t.Run("no next, unmerge", func() {
		t.Run("done true after count", func() {
			im, dm, cm, _, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()

			iu()
			iu()
			iu()

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(0))

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			t.False(cu())
			t.False(cu())
			t.False(cu())

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.True(isDone)
		})

		t.Run("done false with count", func() {
			im, dm, cm, _, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			t.False(cm(0))
			t.False(cm(0))
			t.False(cu())
			t.False(cu())

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			t.False(cm(0))
			t.True(cu())
		})

		t.Run("done false before count", func() {
			im, dm, cm, _, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(0))

			t.False(cu())
			t.False(cu())
			t.True(cu())
		})
	})

	t.Run("next, no unmerge", func() {
		t.Run("with 1 next; not done", func() {
			im, dm, cm, _, _, du, _ := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(1))
		})

		t.Run("with 1 next; done", func() {
			im, dm, cm, em, _, du, _ := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(1))

			t.True(em())
		})

		t.Run("with 2 next; done", func() {
			im, dm, cm, em, _, du, _ := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(1))
			t.False(cm(1))

			t.False(em())
			t.True(em())
		})

		t.Run("done after count", func() {
			im, dm, cm, em, _, du, _ := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()

			t.False(cm(1))
			t.False(cm(1))
			t.False(em())
			t.False(em())

			t.False(cm(0))

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(0, ru)
			t.True(isDone)
		})
	})

	t.Run("next, unmerge", func() {
		t.Run("with 1 next; not done", func() {
			im, dm, cm, _, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(0))

			t.False(cu())
			t.False(cu())
		})

		t.Run("with 1 next; done", func() {
			im, dm, cm, em, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(0))
			t.False(cm(1))
			t.False(em())

			t.False(cu())
			t.False(cu())
			t.True(cu())
		})

		t.Run("with 2 next; done", func() {
			im, dm, cm, em, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.False(isDone)

			t.False(cm(0))
			t.False(cm(1))
			t.False(cm(1))

			t.False(cu())
			t.False(cu())
			t.False(cu())

			t.False(em())
			t.True(em())
		})

		t.Run("done after count", func() {
			im, dm, cm, em, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

			im()
			im()
			im()
			iu()
			iu()
			iu()

			t.False(cm(1))
			t.False(cm(1))
			t.False(em())
			t.False(em())

			t.False(cm(0))

			rm, isDone := dm()
			t.EqualValues(3, rm)
			t.False(isDone)

			t.False(cu())
			t.False(cu())
			t.False(cu())

			ru, isDone := du()
			t.EqualValues(3, ru)
			t.True(isDone)
		})
	})
}

func (t *testXMergeJobCounter) TestPermutation() {
	sets := [][2]uint64{
		{0, 0},
		{0, 0},
		{1, 1},
		{1, 1},
		{2, 0},
		{2, 0},
		{3, 0},
		{3, 0},
		{4, 0},
		{4, 0},
	}

	run := func(seq []int, verbose bool) error {
		defer goleak.VerifyNone(t.T())

		logf := func(s string, a ...any) {}

		if verbose {
			logf = func(s string, a ...any) {
				t.T().Logf(s, a...)
			}
		}

		var menough, uenough uint64
		var isDone bool

		im, dm, cm, em, iu, du, cu := newXMergeJobCounter(func() {}, func() {})

		for i := range slices.Values(seq) {
			s := sets[i]

			var j bool

			switch s[0] {
			case 0:
				im()
				menough++

				if menough == 2 {
					rm, i := dm()

					if i {
						if isDone {
							return errors.New("done: already done")
						}

						isDone = true

						logf("merge done: %v, %d", i, rm)
					}
				}
			case 1:
				j = cm(s[1])
			case 2:
				j = em()
			case 3:
				iu()
				uenough++

				if uenough == 2 {
					ru, i := du()

					if i {
						if isDone {
							return errors.New("done: already done")
						}

						isDone = true

						logf("unmerge done: %v, %d", i, ru)
					}
				}
			case 4:
				j = cu()
			}

			if j {
				if isDone {
					return errors.New("already done")
				}

				isDone = true

				logf("%v: %v", s, j)
			}
		}

		if !t.True(isDone) {
			return errors.New("t.True(isDone)")
		}

		return nil
	}

	// run([]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, true)

	gen := combin.NewPermutationGenerator(len(sets), len(sets))

	var count int

	for gen.Next() {
		count++

		if count%36 != 0 {
			continue
		}

		seq := gen.Permutation(nil)

		if count > 0 && count%999 == 0 {
			fmt.Fprintf(os.Stdout, "%d ", count)
		}

		t.NoError(run(seq, false), "%v", seq)
	}
	fmt.Fprintf(os.Stdout, "%d\n", count)
}

func TestXMergeJobCounter(t *testing.T) {
	suite.Run(t, new(testXMergeJobCounter))
}

type testMergeJobCounter struct {
	suite.Suite
}

func (t *testMergeJobCounter) TestDone() {
	t.Run("done after 0 count", func() {
		in, _, _, d := newMergeJobCounter()

		in()
		in()
		in()

		c0, isDone := d()
		t.EqualValues(3, c0)
		t.False(isDone)
	})

	t.Run("0 count after done", func() {
		in, _, _, d := newMergeJobCounter()

		in()
		in()

		c0, isDone := d()
		t.EqualValues(2, c0)
		t.False(isDone)

		in()

		c0, isDone = d()
		t.EqualValues(2, c0)
		t.False(isDone)
	})

	t.Run("done before 0 count", func() {
		in, _, _, d := newMergeJobCounter()

		c0, isDone := d()
		t.EqualValues(0, c0)
		t.True(isDone)

		in()

		c0, isDone = d()
		t.EqualValues(0, c0)
		t.False(isDone)
	})
}

func (t *testMergeJobCounter) TestCheck() {
	t.Run("no next", func() {
		t.Run("done true after count", func() {
			in, c, _, d := newMergeJobCounter()

			in()
			in()
			in()

			t.False(c(0))
			t.False(c(0))
			t.False(c(0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.True(isDone)
		})

		t.Run("done false with count", func() {
			in, c, _, d := newMergeJobCounter()

			in()
			in()
			in()

			t.False(c(0))
			t.False(c(0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.True(c(0))
		})

		t.Run("done false before count", func() {
			in, c, _, d := newMergeJobCounter()

			in()
			in()
			in()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(c(0))
			t.False(c(0))
			t.True(c(0))
		})
	})

	t.Run("next", func() {
		t.Run("with 1 next; not done", func() {
			in, c, _, d := newMergeJobCounter()

			in()
			in()
			in()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(c(0))
			t.False(c(0))
			t.False(c(1))
		})

		t.Run("with 1 next; done", func() {
			in, c, e, d := newMergeJobCounter()

			in()
			in()
			in()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(c(0))
			t.False(c(0))
			t.False(c(1))

			t.True(e())
		})

		t.Run("with 2 next; done", func() {
			in, c, e, d := newMergeJobCounter()

			in()
			in()
			in()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(c(0))
			t.False(c(1))
			t.False(c(1))

			t.False(e())
			t.True(e())
		})

		t.Run("done after count", func() {
			in, c, e, d := newMergeJobCounter()

			in()
			in()
			in()

			t.False(c(1))
			t.False(c(1))
			t.False(e())
			t.False(e())

			t.False(c(0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.True(isDone)
		})
	})
}

func (t *testMergeJobCounter) TestPermutation() {
	sets := [][2]uint64{
		{0, 0},
		{0, 0},
		{0, 0},
		{1, 1},
		{1, 1},
		{1, 0},
		{2, 0},
		{2, 0},
	}

	run := func(seq []int, verbose bool) error {
		logf := func(s string, a ...any) {}

		if verbose {
			logf = func(s string, a ...any) {
				t.T().Logf(s, a...)
			}
		}

		defer goleak.VerifyNone(t.T())

		var zeros uint64
		var isDone bool

		in, c, e, d := newMergeJobCounter()

		for i := range slices.Values(seq) {
			s := sets[i]

			var j bool

			switch s[0] {
			case 0:
				in()
				zeros++

				if zeros == 3 {
					c0, i := d()

					if i {
						if isDone {
							panic("done: already done")
						}

						isDone = true

						logf("done: %v, %d", i, c0)
					}
				}
			case 1:
				j = c(s[1])
			case 2:
				j = e()
			}

			if j {
				if isDone {
					panic("pc: already done")
				}

				isDone = true

				logf("pc: %v: %v", s, j)
			}
		}

		if !t.True(isDone) {
			return errors.New("t.True(isDone)")
		}

		return nil
	}

	// run([]int{0, 1, 2, 3, 5, 6, 4, 7}, true)

	gen := combin.NewPermutationGenerator(len(sets), len(sets))

	var count int

	for gen.Next() {
		seq := gen.Permutation(nil)
		count++

		if count > 0 && count%999 == 0 {
			fmt.Fprintf(os.Stdout, "%d ", count)
		}

		t.NoError(run(seq, false), "%v", seq)
	}
	fmt.Fprintf(os.Stdout, "%d\n", count)
}

func TestMergeJobCounter(t *testing.T) {
	suite.Run(t, new(testMergeJobCounter))
}

type testPhaseJobCounter struct {
	suite.Suite
}

func (t *testPhaseJobCounter) TestDone() {
	t.Run("done after 0 count", func() {
		p0, _, d, c := newPhaseJobCounter()
		defer c()

		p0()
		p0()
		p0()

		c0, isDone := d()
		t.EqualValues(3, c0)
		t.False(isDone)
	})

	t.Run("0 count after done", func() {
		p0, _, d, c := newPhaseJobCounter()
		defer c()

		p0()
		p0()

		c0, isDone := d()
		t.EqualValues(2, c0)
		t.False(isDone)

		p0()

		c0, isDone = d()
		t.EqualValues(2, c0)
		t.False(isDone)
	})

	t.Run("done before 0 count", func() {
		p0, _, d, c := newPhaseJobCounter()
		defer c()

		c0, isDone := d()
		t.EqualValues(0, c0)
		t.True(isDone)

		p0()

		c0, isDone = d()
		t.EqualValues(0, c0)
		t.False(isDone)
	})
}

func (t *testPhaseJobCounter) TestCheck() {
	t.Run("no next", func() {
		t.Run("done true after count", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			t.False(pc(0, 0))
			t.False(pc(0, 0))
			t.False(pc(0, 0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.True(isDone)
		})

		t.Run("done false with count", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			t.False(pc(0, 0))
			t.False(pc(0, 0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.True(pc(0, 0))
		})

		t.Run("done false before count", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(pc(0, 0))
			t.False(pc(0, 0))
			t.True(pc(0, 0))
		})
	})

	t.Run("next", func() {
		t.Run("with 1 next; not done", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(pc(0, 0))
			t.False(pc(0, 0))
			t.False(pc(0, 1))
		})

		t.Run("with 1 next; done", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(pc(0, 0))
			t.False(pc(0, 0))
			t.False(pc(0, 1))

			t.True(pc(1, 0))
		})

		t.Run("with 1 next; done", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.False(isDone)

			t.False(pc(0, 0))
			t.False(pc(0, 0))
			t.False(pc(0, 1))

			t.False(pc(1, 1))
			t.False(pc(2, 1))
			t.True(pc(3, 0))
		})

		t.Run("done after count", func() {
			p0, pc, d, c := newPhaseJobCounter()
			defer c()

			p0()
			p0()
			p0()

			t.False(pc(0, 0))

			t.False(pc(0, 1))
			t.False(pc(1, 1))
			t.False(pc(2, 1))
			t.False(pc(3, 0))

			t.False(pc(0, 0))

			c0, isDone := d()
			t.EqualValues(3, c0)
			t.True(isDone)
		})
	})
}

func (t *testPhaseJobCounter) TestPermutation() {
	sets := [][2]uint64{
		{0, 0},
		{0, 1},
		{0, 1},
		{1, 1},
		{1, 1},
		{2, 1},
		{2, 0},
		{3, 0},
	}

	run := func(seq []int, verbose bool) error {
		logf := func(s string, a ...any) {}

		if verbose {
			logf = func(s string, a ...any) {
				t.T().Logf(s, a...)
			}
		}

		defer goleak.VerifyNone(t.T())

		var zeros uint64
		var isDone bool

		p0, pc, d, c := newPhaseJobCounter()
		defer c()

		for i := range slices.Values(seq) {
			s := sets[i]
			if s[0] == 0 {
				p0()
				zeros++

			}

			j := pc(uint8(s[0]), s[1])
			if j {
				if isDone {
					panic("pc: already done")
				}

				isDone = true

				logf("pc: %v: %v", s, j)
			}

			if zeros == 3 {
				c0, i := d()

				if i {
					if isDone {
						panic("done: already done")
					}

					isDone = true

					logf("done: %v, %d", i, c0)
				}
			}

		}

		if !t.True(isDone) {
			return errors.New("t.True(isDone)")
		}

		return nil
	}

	// run([]int{0, 1, 2, 3, 5, 6, 4, 7}, true)

	gen := combin.NewPermutationGenerator(len(sets), len(sets))

	var count int

	for gen.Next() {
		seq := gen.Permutation(nil)
		count++

		if count > 0 && count%999 == 0 {
			fmt.Fprintf(os.Stdout, "%d ", count)
		}

		t.NoError(run(seq, false), "%v", seq)
	}
	fmt.Fprintf(os.Stdout, "%d\n", count)
}

func TestPhaseJobCounter(t *testing.T) {
	suite.Run(t, new(testPhaseJobCounter))
}
