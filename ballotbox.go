package mitum

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"github.com/spikeekips/mitum/logging"
)

// Ballotbox collects ballots and keeps track of majority.
type Ballotbox struct {
	sync.RWMutex
	*logging.Logger
	vrs       *sync.Map
	threshold Threshold
}

func NewBallotbox(threshold Threshold) *Ballotbox {
	return &Ballotbox{
		Logger: logging.NewLogger(func(c zerolog.Context) zerolog.Context {
			return c.Str("module", "ballotbox")
		}),
		vrs:       &sync.Map{},
		threshold: threshold,
	}
}

// Vote receives Ballot and returns VoteResult, which has VoteResult.Result()
// and VoteResult.Majority().
func (bb *Ballotbox) Vote(ballot Ballot) (VoteResult, error) {
	vrs := bb.loadVoteResult(ballot, true)

	return vrs.Vote(ballot, bb.threshold)
}

func (bb *Ballotbox) VoteRecord(ballot Ballot) (VoteRecord, bool) {
	vrs := bb.loadVoteResult(ballot, false)
	if vrs == nil {
		return nil, false
	}

	return vrs.VoteRecord(ballot.Node())
}

func (bb *Ballotbox) loadVoteResult(ballot Ballot, ifNotCreate bool) *VoteRecords {
	bb.Lock()
	defer bb.Unlock()

	key := bb.vrsKey(ballot)

	var vrs *VoteRecords
	if i, found := bb.vrs.Load(key); found {
		vrs = i.(*VoteRecords)
	} else if ifNotCreate {
		vrs = NewVoteRecords(ballot)
		bb.vrs.Store(key, vrs)
	}

	return vrs
}

func (bb *Ballotbox) vrsKey(ballot Ballot) string {
	return fmt.Sprintf("%d-%d-%d", ballot.Height(), ballot.Round(), ballot.Stage())
}