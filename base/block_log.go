package base

import (
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/util/hint"
)

func ManifestLog(m Manifest) *zerolog.Event {
	if m == nil {
		return zerolog.Dict()
	}

	e := zerolog.Dict().
		Stringer("hash", m.Hash()).
		Interface("height", m.Height()).
		Stringer("operations_tree", m.OperationsTree()).
		Stringer("states_tree", m.StatesTree()).
		Stringer("suffrage", m.Suffrage()).
		Time("proposed_at", m.ProposedAt())

	if i, ok := m.(hint.Hinter); ok {
		e.Stringer("hint", i.Hint())
	}

	return e
}
