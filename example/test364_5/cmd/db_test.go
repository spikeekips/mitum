package main

import (
	"context"

	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
)

type baseTestDB struct {
	dutil.BaseSuite
	enc *jsonenc.Encoder
}

func (t *baseTestDB) SetupSuite() {
	t.enc = jsonenc.NewEncoder(context.Background(), 1<<9)
	t.NoError(jsonenc.AddDecode(t.enc, base.MPublickeyHint, &base.MPublickey{}, dop5.DecodeJSONMPublickey))
	t.NoError(jsonenc.AddDecode(t.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, dop5.AccountOpFactDecodeJSON))
}

func (t *baseTestDB) TearDownSuite() {
	t.enc.Close()
}

func (t *baseTestDB) randomFacts(n int) []dop5.OperationFact {
	f := make([]dop5.OperationFact, n)

	for i := range n {
		f[i] = dop5.NewAccountOpFact(
			util.UUID().Bytes(),
			int64(i),
			base.NewMPrivatekey().Publickey(),
			base.NewMPrivatekey().Publickey(),
		)
	}

	return f
}
