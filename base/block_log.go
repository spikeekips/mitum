package base

import "github.com/rs/zerolog"

func ManifestLog(m Manifest) *zerolog.Event {
	if m == nil {
		return zerolog.Dict()
	}

	return zerolog.Dict().
		Stringer("hint", m.Hint()).
		Stringer("hash", m.Hash()).
		Interface("height", m.Height()).
		Stringer("operations_tree", m.OperationsTree()).
		Stringer("states_tree", m.StatesTree()).
		Stringer("suffrage", m.Suffrage()).
		Time("created_at", m.CreatedAt()).
		Time("node_created_at", m.NodeCreatedAt())
}
