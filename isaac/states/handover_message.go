package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	HandoverMessageReadyHint               = hint.MustNewHint("handover-ready-message-v0.0.1")
	HandoverMessageReadyResponseHint       = hint.MustNewHint("handover-ready-response-message-v0.0.1")
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

type HandoverMessageReady struct {
	point base.StagePoint
	baseHandoverMessage
}

func newHandoverMessageReady(id string, point base.StagePoint) HandoverMessageReady {
	return HandoverMessageReady{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageReadyHint, id),
		point:               point,
	}
}

func (h HandoverMessageReady) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageReady")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := h.point.IsValid(nil); err != nil {
		return e.Wrapf(err, "invalid point")
	}

	return nil
}

func (h HandoverMessageReady) Point() base.StagePoint {
	return h.point
}

type HandoverMessageReadyResponse struct {
	err   error
	point base.StagePoint
	baseHandoverMessage
	ok bool
}

func newHandoverMessageReadyResponse(id string, point base.StagePoint, ok bool, err error) HandoverMessageReadyResponse {
	return HandoverMessageReadyResponse{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageReadyResponseHint, id),
		point:               point,
		ok:                  ok,
		err:                 err,
	}
}

func (h HandoverMessageReadyResponse) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageReadyResponse")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if err := h.point.IsValid(nil); err != nil {
		return e.Errorf("point")
	}

	return nil
}

func (h HandoverMessageReadyResponse) Point() base.StagePoint {
	return h.point
}

func (h HandoverMessageReadyResponse) OK() bool {
	return h.ok
}

func (h HandoverMessageReadyResponse) Err() error {
	return h.err
}

type HandoverMessageFinish struct {
	vp base.INITVoteproof
	baseHandoverMessage
}

func newHandoverMessageFinish(id string, vp base.INITVoteproof) HandoverMessageFinish {
	return HandoverMessageFinish{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageFinishHint, id),
		vp:                  vp,
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
	}

	return nil
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

func newHandoverMessageChallengeBlockMap(id string, point base.StagePoint, m base.BlockMap) HandoverMessageChallengeBlockMap {
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
	i interface{}
	baseHandoverMessage
}

func newHandoverMessageData(id string, i interface{}) HandoverMessageData {
	return HandoverMessageData{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageDataHint, id),
		i:                   i,
	}
}

func (h HandoverMessageData) IsValid(b []byte) error {
	e := util.ErrInvalid.Errorf("invalid HandoverMessageData")

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	if i, ok := h.i.(util.IsValider); ok {
		if err := i.IsValid(b); err != nil {
			return e.Errorf("data")
		}
	}

	return nil
}

func (h HandoverMessageData) Data() interface{} {
	return h.i
}

type HandoverMessageCancel struct {
	baseHandoverMessage
}

func newHandoverMessageCancel(id string) HandoverMessageCancel {
	return HandoverMessageCancel{
		baseHandoverMessage: newBaseHandoverMessage(HandoverMessageCancelHint, id),
	}
}

func (h HandoverMessageCancel) IsValid([]byte) error {
	if len(h.id) < 1 {
		return util.ErrInvalid.Errorf("invalid HandoverMessageCancel; empty id")
	}

	return nil
}
