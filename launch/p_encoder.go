package launch

import (
	"context"

	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/ps"
)

var (
	PNameEncoder       = ps.Name("encoder")
	PNameAddHinters    = ps.Name("add-hinters")
	EncodersContextKey = util.ContextKey("encoders")
	EncoderContextKey  = util.ContextKey("encoder")
)

func PEncoder(pctx context.Context) (context.Context, error) {
	e := util.StringError("prepare encoders")

	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()

	if err := encs.AddEncoder(enc); err != nil {
		return pctx, e.Wrap(err)
	}

	return util.ContextWithValues(pctx, map[util.ContextKey]interface{}{
		EncodersContextKey: encs,
		EncoderContextKey:  enc,
	}), nil
}

func PAddHinters(pctx context.Context) (context.Context, error) {
	e := util.StringError("add hinters")

	var encs *encoder.Encoders
	if err := util.LoadFromContextOK(pctx, EncodersContextKey, &encs); err != nil {
		return pctx, e.Wrap(err)
	}

	if err := LoadHinters(encs); err != nil {
		return pctx, e.Wrap(err)
	}

	return pctx, nil
}
