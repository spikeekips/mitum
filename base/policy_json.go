package base

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type BasePolicyJSONMarshaler struct {
	hint.BaseHinter
	NetworkID NetworkID `json:"network_id"`
	Threshold Threshold `json:"threshold"`
}

func (p *BasePolicy) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(BasePolicyJSONMarshaler{
		BaseHinter: p.BaseHinter,
		NetworkID:  p.networkID,
		Threshold:  p.threshold,
	})
}

func (p *BasePolicy) UnmarshalJSON(b []byte) error {
	var u BasePolicyJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return errors.Wrap(err, "failed to unmarshal BasePolicy")
	}

	p.networkID = u.NetworkID
	p.threshold = u.Threshold

	return nil
}
