package fixedtree

import (
	"encoding/json"

	"github.com/spikeekips/mitum/util"
)

func (p Proof) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(p.nodes)
}

func (p *Proof) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal Proof")

	var u []json.RawMessage
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	p.nodes = make([]Node, len(u))

	for i := range u {
		var un BaseNode
		if err := util.UnmarshalJSON(u[i], &un); err != nil {
			return e(err, "")
		}

		p.nodes[i] = un
	}

	return nil
}
