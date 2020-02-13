package isaac

import (
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/logging"
)

// Ballotbox collects ballots and keeps track of majority.
type Ballotbox struct {
	sync.RWMutex
	*logging.Logger
	vrs        *sync.Map
	localState *LocalState
}

func NewBallotbox(localState *LocalState) *Ballotbox {
	return &Ballotbox{
		Logger: logging.NewLogger(func(c zerolog.Context) zerolog.Context {
			return c.Str("module", "ballotbox")
		}),
		vrs:        &sync.Map{},
		localState: localState,
	}
}

// Vote receives Ballot and returns VoteRecords, which has VoteRecords.Result()
// and VoteRecords.Majority().
func (bb *Ballotbox) Vote(ballot Ballot) (VoteProof, error) {
	if !ballot.Stage().CanVote() {
		return nil, xerrors.Errorf("this ballot is not for voting; stage=%s", ballot.Stage())
	}

	vrs := bb.loadVoteRecords(ballot, true)

	vp := vrs.Vote(ballot)

	if vp.IsFinished() && !vp.IsClosed() {
		// TODO Cleaning VoteRecords may take too long time.
		if err := bb.clean(vp.Height(), vp.Round()); err != nil {
			return nil, err
		}
	}

	return vp, nil
}

func (bb *Ballotbox) loadVoteRecords(ballot Ballot, ifNotCreate bool) *VoteRecords {
	bb.Lock()
	defer bb.Unlock()

	key := bb.vrsKey(ballot)

	var vrs *VoteRecords
	if i, found := bb.vrs.Load(key); found {
		vrs = i.(*VoteRecords)
	} else if ifNotCreate {
		vrs = NewVoteRecords(ballot, bb.localState.Policy().Threshold())
		bb.vrs.Store(key, vrs)
	}

	return vrs
}

func (bb *Ballotbox) clean(height Height, round Round) error {
	gh := height.Int64()
	gr := round.Uint64()

	var err error
	var removes []interface{}
	bb.vrs.Range(func(k, v interface{}) bool {
		var h int64
		var r uint64
		var s uint8

		var n int
		n, err = fmt.Sscanf(k.(string), "%d-%d-%d", &h, &r, &s)
		if err != nil {
			return false
		}
		if n != 3 {
			err = xerrors.Errorf("invalid formatted key found: key=%q", k)
			return false
		}

		if h != gh {
			removes = append(removes, k)
		}
		if r != gr {
			removes = append(removes, k)
		}

		return true
	})

	if err != nil {
		return err
	}

	if len(removes) < 1 {
		return nil
	}
	for _, k := range removes {
		bb.vrs.Delete(k)
	}

	return nil
}

func (bb *Ballotbox) vrsKey(ballot Ballot) string {
	return fmt.Sprintf("%d-%d-%d", ballot.Height(), ballot.Round(), ballot.Stage())
}