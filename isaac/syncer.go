package isaac

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
)

var ErrRetrySyncSources = util.NewError("sync sources problem; will retry")

type Syncer interface {
	Add(base.Height) bool
	Finished() <-chan base.Height
	Done() <-chan struct{} // revive:disable-line:nested-structs
	Err() error
	IsFinished() (base.Height, bool)
	Cancel() error
}

func RetrySyncSource(ctx context.Context, f func() (bool, error), limit int, interval time.Duration) error {
	return util.Retry(
		ctx,
		func() (bool, error) {
			keep, err := f()

			switch {
			case err == nil:
			case errors.Is(err, ErrRetrySyncSources),
				quicstream.IsNetworkError(err):
				return true, nil
			}

			return keep, err
		},
		limit,
		interval,
	)
}
