package launch

import (
	"os"
	"runtime/pprof"

	"github.com/spikeekips/mitum/util"
)

func StartProfile(cpu, mem string) (func() error, error) {
	e := util.StringErrorFunc("failed to start profile")

	cpuf, err := os.Create(cpu)
	if err != nil {
		return nil, e(err, "could not create CPU profile")
	}

	if err = pprof.StartCPUProfile(cpuf); err != nil {
		defer func() {
			_ = cpuf.Close()
		}()

		return nil, e(err, "could not start CPU profile")
	}

	memf, err := os.Create(mem)
	if err != nil {
		return nil, e(err, "could not create memory profile")
	}

	if err := pprof.WriteHeapProfile(memf); err != nil {
		defer func() {
			_ = memf.Close()
		}()

		return nil, e(err, "could not write memory profile")
	}

	return func() error {
		defer func() {
			_ = cpuf.Close()
		}()

		defer pprof.StopCPUProfile()

		defer func() {
			_ = memf.Close()
		}()

		return nil
	}, nil
}
