package isaacstates

import (
	"context"
	"sync"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	ErrHandoverCanceled = util.NewMError("handover canceled")
	errHandoverIgnore   = util.NewMError("ignore")
	errHandoverReset    = util.NewMError("wrong")
)

var (
	defaultHandoverXMinChallengeCount uint64 = 2
	defaultHandoverXReadyEnd          uint64 = 3
)

// FIXME WhenFinished; left memberlist

type HandoverXBrokerArgs struct {
	SendFunc          func(context.Context, interface{}) error
	CheckIsReady      func() (bool, error)
	WhenCanceled      func(error)
	WhenFinished      func(base.INITVoteproof) error
	GetProposal       func(proposalfacthash util.Hash) (base.ProposalSignFact, bool, error)
	Local             base.Node
	NetworkID         base.NetworkID
	MinChallengeCount uint64
	ReadyEnd          uint64
}

func NewHandoverXBrokerArgs(local base.Node, networkID base.NetworkID) *HandoverXBrokerArgs {
	return &HandoverXBrokerArgs{
		Local:             local,
		NetworkID:         networkID,
		MinChallengeCount: defaultHandoverXMinChallengeCount,
		SendFunc: func(context.Context, interface{}) error {
			return ErrHandoverCanceled.Errorf("SendFunc not implemented")
		},
		CheckIsReady: func() (bool, error) { return false, util.ErrNotImplemented.Errorf("CheckIsReady") },
		WhenCanceled: func(error) {},
		ReadyEnd:     defaultHandoverXReadyEnd,
		WhenFinished: func(base.INITVoteproof) error { return nil },
		GetProposal: func(util.Hash) (base.ProposalSignFact, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetProposal")
		},
	}
}

// HandoverXBroker handles handover processes of consensus node.
type HandoverXBroker struct {
	lastVoteproof   base.Voteproof
	lastReceived    interface{}
	cancelByMessage func()
	cancel          func(error)
	stop            func()
	whenCanceledf   func(error)
	whenFinishedf   func(base.INITVoteproof) error
	*logging.Logging
	ctxFunc               func() context.Context
	args                  *HandoverXBrokerArgs
	successcount          *util.Locked[uint64]
	id                    string
	previousReadyHandover base.StagePoint
	readyEnd              uint64
	challengecount        uint64
	lastchallengecount    uint64
}

func NewHandoverXBroker(ctx context.Context, args *HandoverXBrokerArgs) *HandoverXBroker {
	hctx, cancel := context.WithCancel(ctx)

	id := util.ULID().String()

	var cancelOnce sync.Once

	h := &HandoverXBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-x-broker").Str("id", id)
		}),
		args:          args,
		id:            id,
		ctxFunc:       func() context.Context { return hctx },
		successcount:  util.EmptyLocked[uint64](),
		whenCanceledf: func(error) {},
		whenFinishedf: func(base.INITVoteproof) error { return nil },
	}

	h.cancel = func(err error) {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Err(err).Msg("canceled")

			_ = args.SendFunc(ctx, newHandoverMessageCancel(id))

			cancel()

			h.whenCanceled(err)
		})
	}

	h.cancelByMessage = func() {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Msg("canceled by message")

			cancel()

			h.whenCanceled(ErrHandoverCanceled.Errorf("canceled by message"))
		})
	}

	h.stop = func() {
		cancelOnce.Do(func() {
			defer h.Log().Debug().Msg("stopped")

			cancel()
		})
	}

	return h
}

func (h *HandoverXBroker) ID() string {
	return h.id
}

func (h *HandoverXBroker) isCanceled() error {
	if err := h.ctxFunc().Err(); err != nil {
		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (h *HandoverXBroker) isReady() (uint64, bool) {
	count, _ := h.successcount.Value()

	return count, h.checkIsReady(count)
}

func (h *HandoverXBroker) checkIsReady(count uint64) bool {
	return count >= h.args.MinChallengeCount
}

func (h *HandoverXBroker) isFinished(vp base.Voteproof) (isFinished bool, _ error) {
	if err := h.isCanceled(); err != nil {
		return false, err
	}

	if vp == nil {
		return false, nil
	}

	_ = h.successcount.Get(func(uint64, bool) error {
		if h.readyEnd < 1 {
			return nil
		}

		if _, ok := vp.(base.INITVoteproof); !ok {
			return nil
		}

		switch count, ok := h.isReady(); {
		case !ok:
			return nil
		default:
			isFinished = count >= h.readyEnd

			return nil
		}
	})

	return isFinished, nil
}

func (h *HandoverXBroker) finish(ivp base.INITVoteproof, pr base.ProposalSignFact) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	if err := h.whenFinished(ivp); err != nil {
		h.Log().Error().Err(err).Interface("voteproof", ivp).Msg("failed to when finished for states")

		return err
	}

	defer h.stop()

	hc := newHandoverMessageFinish(h.id, ivp, pr)
	if err := h.args.SendFunc(h.ctxFunc(), hc); err != nil {
		return ErrHandoverCanceled.Wrap(err)
	}

	h.Log().Debug().Interface("message", hc).Msg("sent HandoverMessageFinish")

	return nil
}

// sendVoteproof sends voteproof to Y; If ready to finish, sendVoteproof will
// finish broker process.
func (h *HandoverXBroker) sendVoteproof(ctx context.Context, vp base.Voteproof) (isFinished bool, _ error) {
	if err := h.isCanceled(); err != nil {
		return false, err
	}

	var pr base.ProposalSignFact

	if ivp, ok := vp.(base.INITVoteproof); ok {
		// NOTE send proposal of init voteproof
		if vp.Result() == base.VoteResultMajority {
			switch i, found, err := h.args.GetProposal(ivp.BallotMajority().Proposal()); {
			case err != nil, !found:
			default:
				pr = i
			}
		}

		switch ok, err := h.isFinished(ivp); {
		case err != nil:
			return false, err
		case ok:
			h.Log().Debug().Interface("init_voteproof", ivp).Msg("finished")

			return true, h.finish(ivp, pr)
		}
	}

	switch err := h.sendVoteproofErr(ctx, pr, vp); {
	case err == nil, errors.Is(err, errHandoverIgnore):
		return false, nil
	default:
		h.cancel(err)

		return false, ErrHandoverCanceled.Wrap(err)
	}
}

func (h *HandoverXBroker) sendVoteproofErr(ctx context.Context, pr base.ProposalSignFact, vp base.Voteproof) error {
	_ = h.successcount.Get(func(uint64, bool) error {
		h.lastVoteproof = vp
		h.challengecount++

		return nil
	})

	switch ivp, ok := vp.(base.INITVoteproof); {
	case !ok,
		vp.Result() != base.VoteResultMajority:
		return h.sendData(ctx, HandoverMessageDataTypeVoteproof, vp)
	default:
		return h.sendData(ctx, HandoverMessageDataTypeINITVoteproof, []interface{}{pr, ivp})
	}
}

func (h *HandoverXBroker) sendData(ctx context.Context, dataType HandoverMessageDataType, data interface{}) error {
	if err := h.isCanceled(); err != nil {
		return err
	}

	hc := newHandoverMessageData(h.id, dataType, data)

	switch err := h.args.SendFunc(ctx, hc); {
	case err == nil:
		return nil
	default:
		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}
}

func (h *HandoverXBroker) receive(i interface{}) error {
	_, err := h.successcount.Set(func(before uint64, _ bool) (uint64, error) {
		var after, beforeReadyEnd uint64
		{
			beforeReadyEnd = h.readyEnd
		}

		var err error

		switch after, err = h.receiveInternal(i, before); {
		case err == nil:
		case errors.Is(err, errHandoverIgnore):
			after = before

			err = nil
		case errors.Is(err, errHandoverReset):
			h.readyEnd = 0

			err = nil
		case errors.Is(err, ErrHandoverCanceled):
		default:
			h.cancel(err)

			err = ErrHandoverCanceled.Wrap(err)
		}

		h.Log().Debug().
			Interface("data", i).
			Err(err).
			Uint64("min_challenge_count", h.args.MinChallengeCount).
			Dict("before", zerolog.Dict().
				Uint64("challenge_count", before).
				Uint64("ready_end", beforeReadyEnd),
			).
			Dict("after", zerolog.Dict().
				Uint64("challenge_count", after).
				Uint64("ready_end", h.readyEnd),
			).
			Msg("received")

		return after, err
	})

	return err
}

func (h *HandoverXBroker) receiveInternal(i interface{}, successcount uint64) (uint64, error) {
	if err := h.isCanceled(); err != nil {
		return 0, err
	}

	if id, ok := i.(HandoverMessage); ok {
		if h.id != id.HandoverID() {
			return 0, errors.Errorf("id not matched")
		}
	}

	if iv, ok := i.(util.IsValider); ok {
		if err := iv.IsValid(h.args.NetworkID); err != nil {
			return 0, err
		}
	}

	if _, ok := i.(HandoverMessageCancel); ok {
		h.cancelByMessage()

		return 0, ErrHandoverCanceled.Errorf("canceled by message")
	}

	switch t := i.(type) {
	case HandoverMessageChallengeStagePoint,
		HandoverMessageChallengeBlockMap:
		return h.receiveChallenge(t, successcount)
	default:
		return 0, errHandoverIgnore.Errorf("Y sent unknown message, %T", i)
	}
}

func (h *HandoverXBroker) receiveChallenge(i interface{}, successcount uint64) (uint64, error) {
	var err error

	var point base.StagePoint

	switch t := i.(type) {
	case HandoverMessageChallengeStagePoint:
		point = t.Point()
		err = h.receiveStagePoint(t)
	case HandoverMessageChallengeBlockMap:
		point = t.Point()
		err = h.receiveBlockMap(t)
	default:
		return 0, errHandoverIgnore.Errorf("Y sent unknown message, %T", i)
	}

	if err != nil {
		return 0, err
	}

	after := successcount + 1

	if h.lastchallengecount != h.challengecount-1 {
		after = 1
	}

	h.lastchallengecount = h.challengecount

	if err := h.challengeIsReady(point, after); err != nil {
		return 0, err
	}

	return after, nil
}

func (h *HandoverXBroker) receiveStagePoint(i HandoverMessageChallengeStagePoint) error {
	point := i.Point()

	if err := func() error {
		if h.lastReceived == nil {
			return nil
		}

		if prev, ok := h.lastReceived.(base.StagePoint); ok && point.Compare(prev) < 1 {
			return errHandoverIgnore.Errorf("old stagepoint from y")
		}

		return nil
	}(); err != nil {
		return err
	}

	h.lastReceived = point

	if h.lastVoteproof == nil {
		return errors.Errorf("no last init voteproof for blockmap from y")
	}

	if !isStagePointChallenge(h.lastVoteproof) {
		return errHandoverReset.Errorf("not stagepoint challenge voteproof")
	}

	switch t := h.lastVoteproof.(type) {
	case base.INITVoteproof:
		if !t.Point().Equal(point) {
			return errHandoverReset.Errorf("stagepoint not match for init voteproof")
		}
	case base.ACCEPTVoteproof:
		if !t.Point().Equal(point) {
			return errHandoverReset.Errorf("stagepoint not match for accept voteproof")
		}
	default:
		return errors.Errorf("unknown voteproof, %T", t)
	}

	return nil
}

func (h *HandoverXBroker) receiveBlockMap(i HandoverMessageChallengeBlockMap) error {
	m := i.BlockMap()

	switch {
	case !m.Node().Equal(h.args.Local.Address()):
		return errors.Errorf("invalid blockmap from y; not signed by local; wrong address")
	case !m.Signer().Equal(h.args.Local.Publickey()):
		return errors.Errorf("invalid blockmap from y; not signed by local; different key")
	}

	if err := func() error {
		if h.lastReceived == nil {
			return nil
		}

		prevbm, ok := h.lastReceived.(base.BlockMap)
		if !ok {
			return nil
		}

		if m.Manifest().Height() <= prevbm.Manifest().Height() {
			return errHandoverIgnore.Errorf("old blockmap from y")
		}

		return nil
	}(); err != nil {
		return err
	}

	h.lastReceived = m

	if h.lastVoteproof == nil {
		return errors.Errorf("no last accept voteproof for blockmap from y")
	}

	switch avp, ok := h.lastVoteproof.(base.ACCEPTVoteproof); {
	case !ok:
		return errHandoverReset.Errorf("last not accept voteproof")
	case avp.Result() != base.VoteResultMajority:
		return errHandoverReset.Errorf("last not majority accept voteproof, but blockmap from y")
	case !avp.BallotMajority().NewBlock().Equal(m.Manifest().Hash()):
		return errHandoverReset.Errorf("manifest hash not match")
	default:
		return nil
	}
}

func (h *HandoverXBroker) challengeIsReady(point base.StagePoint, successcount uint64) error {
	err := h.challengeIsReadyOK(point)

	var isReady bool
	if err == nil && h.checkIsReady(successcount) {
		isReady, err = h.args.CheckIsReady()
	}

	if serr := h.args.SendFunc(
		h.ctxFunc(),
		newHandoverMessageChallengeResponse(h.id, point, isReady, err),
	); serr != nil {
		h.readyEnd = 0

		err = util.JoinErrors(err, serr)

		h.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}

	switch {
	case err != nil:
		h.readyEnd = 0

		return err
	case !isReady, h.readyEnd > 0:
	default:
		h.readyEnd = successcount + h.args.ReadyEnd
	}

	return nil
}

func (h *HandoverXBroker) challengeIsReadyOK(point base.StagePoint) error {
	switch vp := h.lastVoteproof; {
	case vp == nil:
		return errors.Errorf("no last voteproof, but HandoverMessageReady from y")
	case point.Compare(vp.Point()) > 0:
		return errHandoverReset.Errorf("higher HandoverMessageReady point with last voteproof")
	}

	switch prev := h.previousReadyHandover; {
	case prev.IsZero():
	case point.Compare(prev) <= 0:
		return errHandoverReset.Errorf(
			"HandoverMessageReady point should be higher than previous")
	}

	h.previousReadyHandover = point

	return nil
}

func (h *HandoverXBroker) whenCanceled(err error) {
	h.whenCanceledf(err)

	h.args.WhenCanceled(err)
}

func (h *HandoverXBroker) whenFinished(vp base.INITVoteproof) error {
	err := h.whenFinishedf(vp)

	return util.JoinErrors(err, h.args.WhenFinished(vp))
}

func (h *HandoverXBroker) patchStates(st *States) error {
	h.whenFinishedf = func(vp base.INITVoteproof) error {
		st.cleanHandoverBrokers()

		_ = st.SetAllowConsensus(false)

		if current := st.current(); current != nil {
			go func() {
				// NOTE moves to syncing
				err := st.AskMoveState(newSyncingSwitchContextWithVoteproof(current.state(), vp))
				if err != nil {
					panic(err)
				}
			}()
		}

		return nil
	}

	h.whenCanceledf = func(error) {
		st.cleanHandoverBrokers()
	}

	return nil
}

func isStagePointChallenge(vp base.Voteproof) bool {
	switch t := vp.(type) {
	case base.INITVoteproof:
		return true
	case base.ACCEPTVoteproof:
		return t.Result() != base.VoteResultMajority
	default:
		return false
	}
}
