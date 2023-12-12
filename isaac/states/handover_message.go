package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

type HandoverMessageDataType string

const (
	HandoverMessageDataTypeUnknown        HandoverMessageDataType = ""
	HandoverMessageDataTypeVoteproof      HandoverMessageDataType = "voteproof"
	HandoverMessageDataTypeINITVoteproof  HandoverMessageDataType = "init_voteproof"
	HandoverMessageDataTypeBallot         HandoverMessageDataType = "ballot"
	HandoverMessageDataTypeProposal       HandoverMessageDataType = "proposal"
	HandoverMessageDataTypeOperation      HandoverMessageDataType = "operation"
	HandoverMessageDataTypeSuffrageVoting HandoverMessageDataType = "suffrage_voting_expel_operation"
)

func (d HandoverMessageDataType) IsValid([]byte) error {
	switch d {
	case
		HandoverMessageDataTypeVoteproof,
		HandoverMessageDataTypeINITVoteproof,
		HandoverMessageDataTypeBallot,
		HandoverMessageDataTypeProposal,
		HandoverMessageDataTypeOperation,
		HandoverMessageDataTypeSuffrageVoting:

	default:
		return util.ErrInvalid.Errorf("unknown handover message data type, %q", d)
	}

	return nil
}

var (
	HandoverMessageChallengeResponseHint   = hint.MustNewHint("handover-challenge-response-message-v0.0.1")
	HandoverMessageFinishHint              = hint.MustNewHint("handover-finish-message-v0.0.1")
	HandoverMessageChallengeStagePointHint = hint.MustNewHint("handover-challenge-stagepoint-message-v0.0.1")
	HandoverMessageChallengeBlockMapHint   = hint.MustNewHint("handover-challenge-blockmap-message-v0.0.1")
	HandoverMessageDataHint                = hint.MustNewHint("handover-data-message-v0.0.1")
	HandoverMessageCancelHint              = hint.MustNewHint("handover-cancel-message-v0.0.1")
)

type HandoverMessage interface {
	util.IsValider
	HandoverID() string
}

type baseHandoverMessage struct {
	id string
	hint.BaseHinter
}

func newBaseHandoverMessage(ht hint.Hint, id string) baseHandoverMessage {
	return baseHandoverMessage{
		BaseHinter: hint.NewBaseHinter(ht),
		id:         id,
	}
}

func (h baseHandoverMessage) HandoverID() string {
	return h.id
}

type HandoverMessageChallengeResponse struct {
	err   error
	point base.StagePoint
	baseHandoverMessage
	ok bool
}

func newHandoverMessageChallengeResponse(
	id string, point base.StagePoint, ok bool, err error,
) HandoverMessageChallengeResponse {
	return HandoverMessageChallengeResponse{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageChallengeResponseHint, id),
		point:               point,
		ok:                  ok,
		err:                 err,
	}
}

func (h HandoverMessageChallengeResponse) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageChallengeResponse")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := h.point.IsValid(nil); err != nil {
		return e.Errorf("point")
	}

	return nil
}

func (h HandoverMessageChallengeResponse) Point() base.StagePoint {
	return h.point
}

func (h HandoverMessageChallengeResponse) OK() bool {
	return h.ok
}

func (h HandoverMessageChallengeResponse) Err() error {
	return h.err
}

type HandoverMessageFinish struct {
	pr base.ProposalSignFact
	vp base.INITVoteproof
	baseHandoverMessage
}

func newHandoverMessageFinish(id string, vp base.INITVoteproof, pr base.ProposalSignFact) HandoverMessageFinish {
	return HandoverMessageFinish{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageFinishHint, id),
		vp:                  vp,
		pr:                  pr,
	}
}

func (h HandoverMessageFinish) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid FinishMessageHandover")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if h.vp != nil {
		if err := h.vp.IsValid(networkID); err != nil {
			return e.Wrap(err)
		}

		if h.pr != nil {
			if err := h.pr.IsValid(networkID); err != nil {
				return e.Wrap(err)
			}
		}
	}

	return nil
}

func (h HandoverMessageFinish) Proposal() base.ProposalSignFact {
	return h.pr
}

func (h HandoverMessageFinish) INITVoteproof() base.INITVoteproof {
	return h.vp
}

type HandoverMessageChallengeStagePoint struct {
	point base.StagePoint
	baseHandoverMessage
}

func newHandoverMessageChallengeStagePoint(id string, point base.StagePoint) HandoverMessageChallengeStagePoint {
	return HandoverMessageChallengeStagePoint{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageChallengeStagePointHint, id),
		point:               point,
	}
}

func (h HandoverMessageChallengeStagePoint) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageChallengeStagePoint")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := h.point.IsValid(nil); err != nil {
		return e.Errorf("point")
	}

	return nil
}

func (h HandoverMessageChallengeStagePoint) Point() base.StagePoint {
	return h.point
}

type HandoverMessageChallengeBlockMap struct {
	m     base.BlockMap
	point base.StagePoint
	baseHandoverMessage
}

func newHandoverMessageChallengeBlockMap(
	id string, point base.StagePoint, m base.BlockMap,
) HandoverMessageChallengeBlockMap {
	return HandoverMessageChallengeBlockMap{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageChallengeBlockMapHint, id),
		point:               point,
		m:                   m,
	}
}

func (h HandoverMessageChallengeBlockMap) IsValid(networkID []byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageChallengeBlockMap")

	switch {
	case len(h.id) < 1:
		return e.Errorf("empty id")
	case h.m == nil:
		return e.Errorf("empty blockmap")
	case h.point.IsZero():
		return e.Errorf("empty point")
	}

	if err := h.m.IsValid(networkID); err != nil {
		return e.Errorf("blockmap")
	}

	if h.m.Manifest().Height() != h.point.Height() {
		return e.Errorf("height not matched")
	}

	return nil
}

func (h HandoverMessageChallengeBlockMap) Point() base.StagePoint {
	return h.point
}

func (h HandoverMessageChallengeBlockMap) BlockMap() base.BlockMap {
	return h.m
}

type HandoverMessageData struct {
	data     interface{}
	dataType HandoverMessageDataType
	baseHandoverMessage
}

func newHandoverMessageData(id string, dataType HandoverMessageDataType, i interface{}) HandoverMessageData {
	return HandoverMessageData{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageDataHint, id),
		data:                i,
		dataType:            dataType,
	}
}

func (h HandoverMessageData) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageData")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := h.dataType.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := h.isValidData(b); err != nil {
		return e.WithMessage(err, "data")
	}

	return nil
}

func (h HandoverMessageData) Data() interface{} {
	return h.data
}

func (h HandoverMessageData) DataType() HandoverMessageDataType {
	return h.dataType
}

func (h HandoverMessageData) LoadVoteproofData() (base.Voteproof, error) {
	return util.AssertInterfaceValue[base.Voteproof](h.data)
}

func (h HandoverMessageData) LoadINITVoteproofData() (pr base.ProposalSignFact, ivp base.INITVoteproof, _ error) {
	e := util.StringError("invalid init voteproof data")

	var i []interface{}

	switch err := util.SetInterfaceValue(h.data, &i); {
	case err != nil:
		return nil, nil, e.Wrap(err)
	case len(i) < 2:
		return nil, nil, e.Errorf("expected [2]interface{}")
	}

	if i[0] != nil {
		if err := util.SetInterfaceValue(i[0], &pr); err != nil {
			return nil, nil, e.Wrap(err)
		}
	}

	if err := util.SetInterfaceValue(i[1], &ivp); err != nil {
		return nil, nil, e.Wrap(err)
	}

	return pr, ivp, nil
}

func (h HandoverMessageData) isValidData(b []byte) error {
	switch h.dataType {
	case HandoverMessageDataTypeVoteproof:
		return h.isValidVoteproof(b)
	case HandoverMessageDataTypeINITVoteproof:
		return h.isValidINITVoteproof(b)
	case HandoverMessageDataTypeBallot:
		return h.isValidBallot(b)
	case HandoverMessageDataTypeProposal:
		return h.isValidProposal(b)
	case HandoverMessageDataTypeOperation:
		return h.isValidOperation(b)
	case HandoverMessageDataTypeSuffrageVoting:
		return h.isValidSuffrageVoting(b)
	default:
		return nil
	}
}

func (h HandoverMessageData) isValidVoteproof(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid voteproof")

	switch vp, err := h.LoadVoteproofData(); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := vp.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h HandoverMessageData) isValidINITVoteproof(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid init voteproof")

	switch pr, vp, err := h.LoadINITVoteproofData(); {
	case err != nil:
		return e.Wrap(err)
	default:
		if pr != nil {
			if err := pr.IsValid(b); err != nil {
				return e.Wrap(err)
			}
		}

		if err := vp.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h HandoverMessageData) isValidBallot(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid ballot")

	switch vp, err := util.AssertInterfaceValue[base.Ballot](h.data); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := vp.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h HandoverMessageData) isValidProposal(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid proposal")

	switch pr, err := util.AssertInterfaceValue[base.ProposalSignFact](h.data); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := pr.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h HandoverMessageData) isValidOperation(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid operation")

	switch op, err := util.AssertInterfaceValue[base.Operation](h.data); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := op.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h HandoverMessageData) isValidSuffrageVoting(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid suffrage voting")

	switch sv, err := util.AssertInterfaceValue[base.SuffrageExpelOperation](h.data); {
	case err != nil:
		return e.Wrap(err)
	default:
		if err := sv.IsValid(b); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

type HandoverMessageCancel struct {
	err error
	baseHandoverMessage
}

func NewHandoverMessageCancel(id string, err error) HandoverMessageCancel {
	return HandoverMessageCancel{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageCancelHint, id),
		err:                 err,
	}
}

func (h HandoverMessageCancel) IsValid([]byte) error {
	if len(h.id) < 1 {
		return util.ErrInvalid.Errorf("invalid HandoverMessageCancel; empty id")
	}

	return nil
}

func (h HandoverMessageCancel) Err() error {
	return h.err
}
