package launch

import (
	"time"

	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util"
)

func init() { //nolint:gochecknoinits //...
	zerolog.TimeFieldFormat = time.RFC3339Nano
	zerolog.ErrorStackMarshaler = util.ZerologMarshalStack
}
