package launch

import (
	"github.com/spikeekips/mitum/util/logging"
)

func SetupLoggingFromFlags(flag Logging) (*logging.Logging, error) {
	logout, err := flag.Out.File()
	if err != nil {
		return nil, err
	}

	return logging.Setup(
		logout,
		flag.Level.Level(),
		flag.Format,
		flag.ForceColor,
	), nil
}
