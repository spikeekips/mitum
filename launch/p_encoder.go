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
	e := util.StringErrorFunc("failed to prepare encoders")

	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()

	if err := encs.AddHinter(enc); err != nil {
		return pctx, e(err, "")
	}

	pctx = context.WithValue(pctx, EncodersContextKey, encs) //revive:disable-line:modifies-parameter
	pctx = context.WithValue(pctx, EncoderContextKey, enc)   //revive:disable-line:modifies-parameter

	return pctx, nil
}

func PAddHinters(pctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to add hinters")

	var enc encoder.Encoder
	if err := util.LoadFromContextOK(pctx, EncoderContextKey, &enc); err != nil {
		return pctx, e(err, "")
	}

	if err := LoadHinters(enc); err != nil {
		return pctx, e(err, "")
	}

	return pctx, nil
}
