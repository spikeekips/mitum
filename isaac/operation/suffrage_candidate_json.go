package isaacoperation

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type suffrageCandidateFactJSONMarshaler struct {
	Address   base.Address   `json:"address"`
	Publickey base.Publickey `json:"publickey"`
	base.BaseFactJSONMarshaler
}

func (fact SuffrageCandidateFact) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(suffrageCandidateFactJSONMarshaler{
		BaseFactJSONMarshaler: fact.BaseFact.JSONMarshaler(),
		Address:               fact.address,
		Publickey:             fact.publickey,
	})
}

type suffrageCandidateFactJSONUnmarshaler struct {
	Address   string `json:"address"`
	Publickey string `json:"publickey"`
	base.BaseFactJSONUnmarshaler
}

func (fact *SuffrageCandidateFact) DecodeJSON(b []byte, enc encoder.Encoder) error {
	e := util.StringError("decode SuffrageCandidateFact")

	var u suffrageCandidateFactJSONUnmarshaler
	if err := enc.Unmarshal(b, &u); err != nil {
		return e.Wrap(err)
	}

	fact.BaseFact.SetJSONUnmarshaler(u.BaseFactJSONUnmarshaler)

	switch i, err := base.DecodeAddress(u.Address, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		fact.address = i
	}

	switch i, err := base.DecodePublickeyFromString(u.Publickey, enc); {
	case err != nil:
		return e.Wrap(err)
	default:
		fact.publickey = i
	}

	return nil
}
