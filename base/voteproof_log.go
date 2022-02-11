package base

import "github.com/rs/zerolog"

func VoteproofLog(vp Voteproof) *zerolog.Event {
	if vp == nil {
		return nil
	}

	return zerolog.Dict().
		Str("id", vp.ID()).
		Object("point", vp.Point())
}
