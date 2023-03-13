package base

import "github.com/rs/zerolog"

func VoteproofLog(vp Voteproof) *zerolog.Event {
	if vp == nil {
		return zerolog.Dict()
	}

	return zerolog.Dict().
		Str("id", vp.ID()).
		Object("point", vp.Point())
}

func VoteproofLogFunc(k string, vp Voteproof) func(*zerolog.Event) {
	return func(e *zerolog.Event) {
		e.Dict(k, VoteproofLog(vp))
	}
}
