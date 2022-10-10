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

func PEncoder(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to prepare encoders")

	encs := encoder.NewEncoders()
	enc := jsonenc.NewEncoder()

	if err := encs.AddHinter(enc); err != nil {
		return ctx, e(err, "")
	}

	ctx = context.WithValue(ctx, EncodersContextKey, encs) //revive:disable-line:modifies-parameter
	ctx = context.WithValue(ctx, EncoderContextKey, enc)   //revive:disable-line:modifies-parameter

	return ctx, nil
}

func PAddHinters(ctx context.Context) (context.Context, error) {
	e := util.StringErrorFunc("failed to add hinters")

	var enc encoder.Encoder
	if err := util.LoadFromContextOK(ctx, EncoderContextKey, &enc); err != nil {
		return ctx, e(err, "")
	}

	if err := LoadHinters(enc); err != nil {
		return ctx, e(err, "")
	}

	return ctx, nil
}
