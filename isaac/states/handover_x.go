package isaacstates

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
)

var (
	ErrHandoverCanceled = util.NewIDError("handover canceled")
	ErrHandoverStopped  = util.NewIDError("handover stopped")
	errHandoverIgnore   = util.NewIDError("ignore")
	errHandoverReset    = util.NewIDError("wrong")
)

var (
	defaultHandoverXMinChallengeCount uint64 = 2
	defaultHandoverXReadyEnd          uint64 = 3
)

type HandoverXBrokerArgs struct {
	SendMessageFunc          func(context.Context, quicstream.ConnInfo, HandoverMessage) error
	CheckIsReady             func() (bool, error)
	WhenCanceled             func(brokerID string, _ error)
	WhenFinished             func(brokerID string, _ base.INITVoteproof, y base.Address, yci quicstream.ConnInfo) error
	GetProposal              func(proposalfacthash util.Hash) (base.ProposalSignFact, bool, error)
	Local                    base.Node
	NetworkID                base.NetworkID
	MinChallengeCount        uint64
	ReadyEnd                 uint64
	CleanAfter               time.Duration
	MaxEnsureSendFailure     uint64
	RetrySendMessageInterval time.Duration
}

func NewHandoverXBrokerArgs(local base.Node, networkID base.NetworkID) *HandoverXBrokerArgs {
	return &HandoverXBrokerArgs{
		Local:             local,
		NetworkID:         networkID,
		MinChallengeCount: defaultHandoverXMinChallengeCount,
		SendMessageFunc: func(context.Context, quicstream.ConnInfo, HandoverMessage) error {
			return ErrHandoverCanceled.Errorf("SendMessageFunc not implemented")
		},
		CheckIsReady: func() (bool, error) { return false, util.ErrNotImplemented.Errorf("CheckIsReady") },
		WhenCanceled: func(string, error) {},
		ReadyEnd:     defaultHandoverXReadyEnd,
		WhenFinished: func(string, base.INITVoteproof, base.Address, quicstream.ConnInfo) error { return nil },
		GetProposal: func(util.Hash) (base.ProposalSignFact, bool, error) {
			return nil, false, util.ErrNotImplemented.Errorf("GetProposal")
		},
		CleanAfter: time.Second * 33, //nolint:gomnd // long enough to handle
		// requests from handover y

		MaxEnsureSendFailure:     9,                     //nolint:gomnd //...
		RetrySendMessageInterval: time.Millisecond * 33, //nolint:gomnd //...
	}
}

// HandoverXBroker handles handover processes of consensus node.
type HandoverXBroker struct {
	lastVoteproof   base.Voteproof
	lastReceived    interface{}
	cancelByMessage func(HandoverMessageCancel)
	cancel          func(error)
	stop            func()
	whenCanceledf   func(error)
	whenFinishedf   func(base.INITVoteproof) error
	*logging.Logging
	ctxFunc                   func() context.Context
	args                      *HandoverXBrokerArgs
	successcount              *util.Locked[uint64]
	isFinishedLocked          *util.Locked[bool]
	sendFailureCount          *util.Locked[uint64]
	connInfo                  quicstream.ConnInfo // NOTE y conn info
	id                        string
	previousChallengeHandover base.StagePoint
	readyEnd                  uint64
	challengecount            uint64
	lastchallengecount        uint64
}

func NewHandoverXBroker(
	ctx context.Context,
	args *HandoverXBrokerArgs,
	connInfo quicstream.ConnInfo,
) *HandoverXBroker {
	hctx, cancel := context.WithCancel(ctx)

	id := util.ULID().String()

	broker := &HandoverXBroker{
		Logging: logging.NewLogging(func(lctx zerolog.Context) zerolog.Context {
			return lctx.Str("module", "handover-x-broker").Str("id", id)
		}),
		args:             args,
		id:               id,
		connInfo:         connInfo,
		ctxFunc:          func() context.Context { return hctx },
		successcount:     util.EmptyLocked[uint64](),
		whenCanceledf:    func(error) {},
		whenFinishedf:    func(base.INITVoteproof) error { return nil },
		isFinishedLocked: util.EmptyLocked[bool](),
		sendFailureCount: util.NewLocked[uint64](0),
	}

	var cancelOnce sync.Once

	broker.cancel = func(err error) {
		cancelOnce.Do(func() {
			defer broker.Log().Debug().Err(err).Msg("canceled")

			go func() {
				_ = broker.retrySendMessage(
					context.Background(), NewHandoverMessageCancel(id, err), 3) //nolint:gomnd //...
			}()

			cancel()

			go broker.whenCanceled(err)
		})
	}

	broker.cancelByMessage = func(i HandoverMessageCancel) {
		cancelOnce.Do(func() {
			defer broker.Log().Debug().Interface("handover_message", i).Msg("canceled by message")

			cancel()

			go broker.whenCanceled(ErrHandoverCanceled.Errorf("canceled by message"))
		})
	}

	broker.stop = func() {
		cancelOnce.Do(func() {
			cancel()
		})
	}

	return broker
}

func (broker *HandoverXBroker) ID() string {
	return broker.id
}

func (broker *HandoverXBroker) ConnInfo() quicstream.ConnInfo {
	return broker.connInfo
}

func (broker *HandoverXBroker) isCanceled() error {
	if err := broker.ctxFunc().Err(); err != nil {
		return ErrHandoverCanceled.Wrap(err)
	}

	return nil
}

func (broker *HandoverXBroker) isReady() (uint64, bool) {
	count, _ := broker.successcount.Value()

	return count, broker.checkIsReady(count)
}

func (broker *HandoverXBroker) checkIsReady(count uint64) bool {
	return count >= broker.args.MinChallengeCount
}

func (broker *HandoverXBroker) isFinished() bool {
	i, _ := broker.isFinishedLocked.Value()

	return i
}

func (broker *HandoverXBroker) isReadyToFinish(vp base.Voteproof) (isFinished bool, _ error) {
	if err := broker.isCanceled(); err != nil {
		return false, err
	}

	if broker.isFinished() {
		return false, ErrHandoverStopped.WithStack()
	}

	if vp == nil {
		return false, nil
	}

	if _, ok := vp.(base.INITVoteproof); !ok {
		return false, nil
	}

	_ = broker.successcount.Get(func(uint64, bool) error {
		if broker.readyEnd < 1 {
			return nil
		}

		switch count, ok := broker.isReady(); {
		case !ok:
			return nil
		default:
			isFinished = count >= broker.readyEnd

			return nil
		}
	})

	return isFinished, nil
}

func (broker *HandoverXBroker) finish(ivp base.INITVoteproof, pr base.ProposalSignFact) error {
	if err := broker.isCanceled(); err != nil {
		return err
	}

	broker.Log().Debug().Msg("trying to finish")

	_, err := broker.isFinishedLocked.Set(func(i, _ bool) (bool, error) {
		if i {
			return false, util.ErrLockedSetIgnore
		}

		defer func() {
			if err := broker.whenFinished(ivp); err != nil {
				broker.Log().Error().Err(err).Msg("failed to finish")
			}
		}()

		hc := newHandoverMessageFinish(broker.id, ivp, pr)

		if err := broker.retrySendMessage(broker.ctxFunc(), hc, 33); err != nil { //nolint:gomnd //...
			return false, err
		}

		broker.Log().Debug().Interface("handover_message", hc).Msg("sent HandoverMessageFinish")

		return true, nil
	})
	if err != nil {
		broker.cancel(err)

		broker.Log().Error().Err(err).Interface("voteproof", ivp).Msg("failed to when finished for states")
	}

	broker.Log().Debug().Msg("finished")

	return err
}

// sendVoteproof sends voteproof to Y; If ready to finish, sendVoteproof will
// finish broker process.
func (broker *HandoverXBroker) sendVoteproof(ctx context.Context, vp base.Voteproof) (isFinished bool, _ error) {
	if err := broker.isCanceled(); err != nil {
		return false, err
	}

	if broker.isFinished() {
		return false, nil
	}

	var pr base.ProposalSignFact

	if ivp, ok := vp.(base.INITVoteproof); ok {
		// NOTE send proposal of init voteproof
		if vp.Result() == base.VoteResultMajority {
			switch i, found, err := broker.args.GetProposal(ivp.BallotMajority().Proposal()); {
			case err != nil, !found:
			default:
				pr = i
			}
		}

		switch ok, err := broker.isReadyToFinish(ivp); {
		case err != nil:
			return false, err
		case ok:
			broker.Log().Debug().Interface("init_voteproof", ivp).Msg("finished")

			if err := broker.finish(ivp, pr); err != nil {
				return false, nil
			}

			return true, nil
		}
	}

	switch err := broker.sendVoteproofErr(ctx, pr, vp); {
	case err == nil, errors.Is(err, errHandoverIgnore):
		return false, nil
	default:
		broker.cancel(err)

		return false, ErrHandoverCanceled.Wrap(err)
	}
}

func (broker *HandoverXBroker) sendVoteproofErr(
	ctx context.Context, pr base.ProposalSignFact, vp base.Voteproof,
) error {
	_ = broker.successcount.Get(func(uint64, bool) error {
		broker.lastVoteproof = vp
		broker.challengecount++

		return nil
	})

	switch ivp, ok := vp.(base.INITVoteproof); {
	case !ok,
		vp.Result() != base.VoteResultMajority:
		return broker.SendData(ctx, HandoverMessageDataTypeVoteproof, vp)
	default:
		return broker.SendData(ctx, HandoverMessageDataTypeINITVoteproof, []interface{}{pr, ivp})
	}
}

func (broker *HandoverXBroker) SendData(ctx context.Context, dataType HandoverMessageDataType, data interface{}) error {
	if err := broker.isCanceled(); err != nil {
		return err
	}

	if broker.isFinished() {
		return nil
	}

	hc := newHandoverMessageData(broker.id, dataType, data)

	switch err := broker.endureSendMessage(ctx, hc); {
	case err == nil:
		return nil
	default:
		broker.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}
}

func (broker *HandoverXBroker) sendBallot(ctx context.Context, ballot base.Ballot) error {
	if err := broker.isCanceled(); err != nil {
		return err
	}

	if broker.isFinished() {
		return nil
	}

	hc := newHandoverMessageData(broker.id, HandoverMessageDataTypeBallot, ballot)

	switch err := broker.endureSendMessage(ctx, hc); {
	case err == nil:
		return nil
	default:
		broker.cancel(err)

		return ErrHandoverCanceled.Wrap(err)
	}
}

func (broker *HandoverXBroker) Receive(i interface{}) error {
	if broker.isFinished() {
		return nil
	}

	_, err := broker.successcount.Set(func(before uint64, _ bool) (uint64, error) {
		var after, beforeReadyEnd uint64
		{
			beforeReadyEnd = broker.readyEnd
		}

		var err error

		switch after, err = broker.receiveInternal(i, before); {
		case err == nil:
		case errors.Is(err, errHandoverIgnore):
			after = before

			err = nil
		case errors.Is(err, errHandoverReset):
			broker.readyEnd = 0

			err = nil
		case errors.Is(err, ErrHandoverCanceled):
		default:
			broker.cancel(err)

			err = ErrHandoverCanceled.Wrap(err)
		}

		broker.Log().Debug().
			Interface("data", i).
			Err(err).
			Uint64("min_challenge_count", broker.args.MinChallengeCount).
			Dict("before", zerolog.Dict().
				Uint64("challenge_count", before).
				Uint64("ready_end", beforeReadyEnd),
			).
			Dict("after", zerolog.Dict().
				Uint64("challenge_count", after).
				Uint64("ready_end", broker.readyEnd),
			).
			Msg("received")

		return after, err
	})

	return err
}

func (broker *HandoverXBroker) receiveInternal(i interface{}, successcount uint64) (uint64, error) {
	if err := broker.isCanceled(); err != nil {
		return 0, err
	}

	if id, ok := i.(HandoverMessage); ok {
		if broker.id != id.HandoverID() {
			return 0, errors.Errorf("id not matched")
		}
	}

	if iv, ok := i.(util.IsValider); ok {
		if err := iv.IsValid(broker.args.NetworkID); err != nil {
			return 0, err
		}
	}

	if msg, ok := i.(HandoverMessageCancel); ok {
		broker.cancelByMessage(msg)

		return 0, ErrHandoverCanceled.Errorf("canceled by message")
	}

	switch t := i.(type) {
	case HandoverMessageChallengeStagePoint,
		HandoverMessageChallengeBlockMap:
		return broker.receiveChallenge(t, successcount)
	default:
		return 0, errHandoverIgnore.Errorf("Y sent unknown message, %T", i)
	}
}

func (broker *HandoverXBroker) receiveChallenge(i interface{}, successcount uint64) (uint64, error) {
	var err error

	var point base.StagePoint

	switch t := i.(type) {
	case HandoverMessageChallengeStagePoint:
		point = t.Point()
		err = broker.receiveStagePoint(t)
	case HandoverMessageChallengeBlockMap:
		point = t.Point()
		err = broker.receiveBlockMap(t)
	default:
		return 0, errHandoverIgnore.Errorf("Y sent unknown message, %T", i)
	}

	if err != nil {
		return 0, err
	}

	after := successcount + 1

	if broker.lastchallengecount != broker.challengecount-1 {
		after = 1
	}

	broker.lastchallengecount = broker.challengecount

	if err := broker.challengeIsReady(point, after); err != nil {
		return 0, err
	}

	return after, nil
}

func (broker *HandoverXBroker) receiveStagePoint(i HandoverMessageChallengeStagePoint) error {
	point := i.Point()

	if err := func() error {
		if broker.lastReceived == nil {
			return nil
		}

		if prev, ok := broker.lastReceived.(base.StagePoint); ok && point.Compare(prev) < 1 {
			return errHandoverIgnore.Errorf("old stagepoint from y")
		}

		return nil
	}(); err != nil {
		return err
	}

	broker.lastReceived = point

	if broker.lastVoteproof == nil {
		return errors.Errorf("no last init voteproof for blockmap from y")
	}

	if !isStagePointChallenge(broker.lastVoteproof) {
		return errHandoverReset.Errorf("not stagepoint challenge voteproof")
	}

	switch t := broker.lastVoteproof.(type) {
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

func (broker *HandoverXBroker) receiveBlockMap(i HandoverMessageChallengeBlockMap) error {
	m := i.BlockMap()

	switch {
	case !m.Node().Equal(broker.args.Local.Address()):
		return errors.Errorf("invalid blockmap from y; not signed by local; wrong address")
	case !m.Signer().Equal(broker.args.Local.Publickey()):
		return errors.Errorf("invalid blockmap from y; not signed by local; different key")
	}

	if err := func() error {
		if broker.lastReceived == nil {
			return nil
		}

		prevbm, ok := broker.lastReceived.(base.BlockMap)
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

	broker.lastReceived = m

	if broker.lastVoteproof == nil {
		return errors.Errorf("no last accept voteproof for blockmap from y")
	}

	switch avp, ok := broker.lastVoteproof.(base.ACCEPTVoteproof); {
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

func (broker *HandoverXBroker) challengeIsReady(point base.StagePoint, successcount uint64) error {
	err := broker.challengeIsReadyOK(point)

	var isReady bool
	if err == nil && broker.checkIsReady(successcount) {
		isReady, err = broker.args.CheckIsReady()
	}

	go func() {
		if serr := broker.endureSendMessage(
			broker.ctxFunc(),
			newHandoverMessageChallengeResponse(broker.id, point, isReady, err),
		); serr != nil {
			broker.cancel(serr)
		}
	}()

	switch {
	case err != nil:
		broker.readyEnd = 0

		return err
	case !isReady, broker.readyEnd > 0:
	default:
		broker.readyEnd = successcount + broker.args.ReadyEnd
	}

	return nil
}

func (broker *HandoverXBroker) challengeIsReadyOK(point base.StagePoint) error {
	switch vp := broker.lastVoteproof; {
	case vp == nil:
		return errors.Errorf("no last voteproof, but HandoverMessageChallenge from y")
	case point.Compare(vp.Point()) > 0:
		return errHandoverReset.Errorf("higher HandoverMessageChallenge point with last voteproof")
	}

	switch prev := broker.previousChallengeHandover; {
	case prev.IsZero():
	case point.Compare(prev) <= 0:
		return errHandoverReset.Errorf(
			"HandoverMessageChallenge point should be higher than previous")
	}

	broker.previousChallengeHandover = point

	return nil
}

func (broker *HandoverXBroker) whenCanceled(err error) {
	broker.whenCanceledf(err)

	broker.args.WhenCanceled(broker.ID(), err)
}

func (broker *HandoverXBroker) whenFinished(vp base.INITVoteproof) (err error) {
	l := broker.Log().With().Interface("voteproof", vp).Logger()

	err = broker.whenFinishedf(vp)

	switch err = util.JoinErrors(
		err,
		broker.args.WhenFinished(broker.ID(), vp, broker.args.Local.Address(), broker.connInfo),
	); {
	case err != nil:
		l.Error().Interface("voteproof", vp).Err(err).Msg("failed to finish")
	default:
		l.Debug().Interface("voteproof", vp).Msg("finished")
	}

	return err
}

func (broker *HandoverXBroker) retrySendMessage(ctx context.Context, msg HandoverMessage, limit int) error {
	return retryHandoverSendMessageFunc(
		ctx,
		limit,
		broker.args.RetrySendMessageInterval,
		broker.args.SendMessageFunc,
		broker.connInfo,
		msg,
	)
}

func (broker *HandoverXBroker) endureSendMessage(ctx context.Context, msg HandoverMessage) error {
	return endureHandoverSendMessageFunc(
		ctx,
		broker.sendFailureCount,
		broker.args.MaxEnsureSendFailure,
		broker.args.SendMessageFunc,
		broker.connInfo,
		msg,
	)
}

func (broker *HandoverXBroker) patchStates(st *States) error {
	broker.whenFinishedf = func(vp base.INITVoteproof) error {
		go func() {
			<-time.After(broker.args.CleanAfter)

			broker.stop()

			st.cleanHandovers()

			broker.Log().Debug().Msg("handover cleaned")
		}()

		_ = st.setAllowConsensus(false)

		if current := st.current(); current != nil {
			go func() {
				// NOTE moves to syncing
				_ = st.AskMoveState(newSyncingSwitchContextWithVoteproof(current.state(), vp))
			}()
		}

		return nil
	}

	broker.whenCanceledf = func(error) {
		st.cleanHandovers()
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

func retryHandoverSendMessageFunc(
	ctx context.Context,
	limit int,
	interval time.Duration,
	f func(context.Context, quicstream.ConnInfo, HandoverMessage) error,
	ci quicstream.ConnInfo,
	m HandoverMessage,
) error {
	return util.Retry(
		ctx,
		func() (bool, error) {
			switch err := f(ctx, ci, m); {
			case err == nil, errors.Is(err, context.Canceled):
				return false, nil
			default:
				return true, err
			}
		},
		limit,
		interval,
	)
}

func endureHandoverSendMessageFunc(
	ctx context.Context,
	count *util.Locked[uint64],
	max uint64,
	f func(context.Context, quicstream.ConnInfo, HandoverMessage) error,
	ci quicstream.ConnInfo,
	m HandoverMessage,
) (err error) {
	prev, _ := count.Value()

	if max > 0 && prev >= max {
		return errors.Errorf("send handover message; over max")
	}

	err = f(ctx, ci, m)
	if err == nil {
		_ = count.SetValue(0)

		return nil
	}

	if prev < max {
		err = nil
	}

	_, _ = count.Set(func(i uint64, _ bool) (uint64, error) {
		return i + 1, nil
	})

	return err
}
