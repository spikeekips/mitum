package base

import (
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type SuffrageInfo interface {
	hint.Hinter
	util.HashByter
	util.Hasher // NOTE hash of suffrage chain
	util.IsValider
	Threshold() Threshold
	Nodes() []Address
}

func IsValidSuffrageInfo(suf SuffrageInfo) error {
	if suf == nil {
		return util.InvalidError.Errorf("nil SuffrageInfo")
	}

	nodes := suf.Nodes()
	if util.CheckSliceDuplicated(nodes, func(i interface{}) string {
		j, ok := i.(Address)
		if !ok {
			return ""
		}

		return j.String()
	}) {
		return util.InvalidError.Errorf("duplicated node found in SuffrageInfo")
	}

	bs := make([]util.IsValider, len(nodes)+2)
	bs[0] = suf.Hash()
	bs[1] = suf.Threshold()
	for i := range nodes {
		bs[i+2] = nodes[i]
	}

	if err := util.CheckIsValid(nil, false, bs...); err != nil {
		return util.InvalidError.Wrapf(err, "invalid SuffrageInfo")
	}

	return nil
}
