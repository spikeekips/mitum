package launch

import (
	"github.com/spikeekips/mitum/isaac"
)

func NodePolicyFromDesign(design NodeDesign) (*isaac.NodePolicy, error) {
	return isaac.DefaultNodePolicy(design.NetworkID), nil
}
