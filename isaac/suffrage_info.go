package isaac

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var SuffrageInfoHint = hint.MustNewHint("suffrage-info-v0.0.1")

type SuffrageInfo struct { // BLOCK remove
	hint.BaseHinter
	state      util.Hash
	height     base.Height
	suffrage   []base.Node
	candidates []base.SuffrageCandidate
}

func NewSuffrageNodesNetworkInfo(
	state util.Hash,
	height base.Height,
	suffrage []base.Node,
	candidates []base.SuffrageCandidate,
) SuffrageInfo {
	return SuffrageInfo{
		BaseHinter: hint.NewBaseHinter(SuffrageInfoHint),
		state:      state,
		height:     height,
		suffrage:   suffrage,
		candidates: candidates,
	}
}

func (info SuffrageInfo) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid SuffrageNodesNetworkInfoHint")

	if err := info.BaseHinter.IsValid(SuffrageInfoHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, info.state, info.height); err != nil {
		return e(err, "")
	}

	if len(info.suffrage) < 1 {
		return e(util.InvalidError.Errorf("empty suffrage"), "")
	}

	sufs := map[string]struct{}{}
	var foundnil bool
	if util.CheckSliceDuplicated(info.suffrage, func(j interface{}) string {
		if j == nil {
			foundnil = true

			return ""
		}

		addr := j.(base.Node).Address().String()
		sufs[addr] = struct{}{}

		return addr
	}) {
		return e(util.InvalidError.Errorf("duplicated suffrage node found"), "")
	}
	if foundnil {
		return e(util.InvalidError.Errorf("empty suffrage node found"), "")
	}

	var foundinsufs bool
	if util.CheckSliceDuplicated(info.candidates, func(j interface{}) string {
		if j == nil {
			foundnil = true

			return ""
		}

		addr := j.(base.SuffrageCandidate).Address().String()
		if _, found := sufs[addr]; found {
			foundinsufs = true
		}

		return addr
	}) {
		return e(util.InvalidError.Errorf("duplicated candidates node found"), "")
	}
	switch {
	case foundnil:
		return e(util.InvalidError.Errorf("empty candidates node found"), "")
	case foundinsufs:
		return e(util.InvalidError.Errorf("candidates node found in suffrage"), "")
	}

	return nil
}

func (info SuffrageInfo) State() util.Hash {
	return info.state
}

func (info SuffrageInfo) Height() base.Height {
	return info.height
}

func (info SuffrageInfo) Suffrage() []base.Node {
	return info.suffrage
}

func (info SuffrageInfo) Candidates() []base.SuffrageCandidate {
	return info.candidates
}
