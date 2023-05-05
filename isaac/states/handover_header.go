package isaacstates

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	StartHandoverHeaderHint       = hint.MustNewHint("start-handover-header-v0.0.1")
	CheckHandoverHeaderHint       = hint.MustNewHint("check-handover-header-v0.0.1")
	AskHandoverHeaderHint         = hint.MustNewHint("ask-handover-header-v0.0.1")
	AskHandoverResponseHeaderHint = hint.MustNewHint("ask-handover-response-header-v0.0.1")
	CancelHandoverHeaderHint      = hint.MustNewHint("cancel-handover-header-v0.0.1")
	HandoverMessageHeaderHint     = hint.MustNewHint("handover-message-header-v0.0.1")
)

var (
	handlerPrefixStartHandoverString   = "start_handover"
	handlerPrefixCheckHandoverString   = "check_handover"
	handlerPrefixAskHandoverString     = "ask_handover"
	handlerPrefixCancelHandoverString  = "cancel_handover"
	handlerPrefixHandoverMessageString = "handover_message"

	HandlerPrefixStartHandover   []byte = quicstream.HashPrefix(handlerPrefixStartHandoverString)
	HandlerPrefixCheckHandover   []byte = quicstream.HashPrefix(handlerPrefixCheckHandoverString)
	HandlerPrefixAskHandover     []byte = quicstream.HashPrefix(handlerPrefixAskHandoverString)
	HandlerPrefixCancelHandover  []byte = quicstream.HashPrefix(handlerPrefixCancelHandoverString)
	HandlerPrefixHandoverMessage []byte = quicstream.HashPrefix(handlerPrefixHandoverMessageString)
)

type baseHeader struct {
	quicstream.BaseRequestHeader
}

func newBaseHeader(ht hint.Hint) baseHeader {
	return baseHeader{BaseRequestHeader: quicstream.NewBaseRequestHeader(ht, headerPrefixByHint(ht))}
}

func (h baseHeader) IsValid([]byte) error {
	return h.BaseHinter.IsValid(h.Hint().Type().Bytes())
}

type caHandoverHeader struct {
	connInfo quicstream.UDPConnInfo // conn info of X
	address  base.Address           // local address
	baseHeader
}

func newCAHandoverHeader(ht hint.Hint, connInfo quicstream.UDPConnInfo, address base.Address) caHandoverHeader {
	return caHandoverHeader{
		baseHeader: newBaseHeader(ht),
		connInfo:   connInfo,
		address:    address,
	}
}

func (h caHandoverHeader) IsValid([]byte) error {
	if err := h.baseHeader.IsValid(nil); err != nil {
		return err
	}

	return util.CheckIsValiders(nil, false, h.connInfo, h.address)
}

func (h caHandoverHeader) ConnInfo() quicstream.UDPConnInfo {
	return h.connInfo
}

func (h caHandoverHeader) Address() base.Address {
	return h.address
}

type StartHandoverHeader struct {
	caHandoverHeader
}

func NewStartHandoverHeader(connInfo quicstream.UDPConnInfo, address base.Address) StartHandoverHeader {
	return StartHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(StartHandoverHeaderHint, connInfo, address),
	}
}

func (h StartHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid StartHandoverHeader")
	}

	return nil
}

type CheckHandoverHeader struct {
	caHandoverHeader
}

func NewCheckHandoverHeader(connInfo quicstream.UDPConnInfo, address base.Address) CheckHandoverHeader {
	return CheckHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(CheckHandoverHeaderHint, connInfo, address),
	}
}

func (h CheckHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid CheckHandoverHeader")
	}

	return nil
}

type AskHandoverHeader struct {
	caHandoverHeader
}

func NewAskHandoverHeader(connInfo quicstream.UDPConnInfo, address base.Address) AskHandoverHeader {
	return AskHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(AskHandoverHeaderHint, connInfo, address),
	}
}

func (h AskHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid AskHandoverHeader")
	}

	return nil
}

type AskHandoverResponseHeader struct {
	id string // id is broker ID
	quicstream.BaseResponseHeader
}

func NewAskHandoverResponseHeader(ok bool, err error, id string) AskHandoverResponseHeader {
	return AskHandoverResponseHeader{
		BaseResponseHeader: quicstream.NewBaseResponseHeader(AskHandoverResponseHeaderHint, ok, err),
		id:                 id,
	}
}

func (h AskHandoverResponseHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("AskHandoverResponseHeader")

	if err := h.BaseResponseHeader.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if len(h.id) < 1 {
		return e.Errorf("empty id")
	}

	return nil
}

func (h AskHandoverResponseHeader) ID() string {
	return h.id
}

type CancelHandoverHeader struct {
	baseHeader
}

func NewCancelHandoverHeader() CancelHandoverHeader {
	return CancelHandoverHeader{
		baseHeader: newBaseHeader(CancelHandoverHeaderHint),
	}
}

func (h CancelHandoverHeader) IsValid([]byte) error {
	if err := h.baseHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid CancelHandoverHeader")
	}

	return nil
}

type HandoverMessageHeader struct {
	baseHeader
}

func NewHandoverMessageHeader() HandoverMessageHeader {
	return HandoverMessageHeader{
		baseHeader: newBaseHeader(HandoverMessageHeaderHint),
	}
}

func (h HandoverMessageHeader) IsValid([]byte) error {
	if err := h.baseHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.Wrapf(err, "invalid HandoverMessageHeader")
	}

	return nil
}

func headerPrefixByHint(ht hint.Hint) []byte {
	switch ht.Type() {
	case StartHandoverHeaderHint.Type():
		return HandlerPrefixStartHandover
	case CheckHandoverHeaderHint.Type():
		return HandlerPrefixCheckHandover
	case AskHandoverHeaderHint.Type():
		return HandlerPrefixAskHandover
	case CancelHandoverHeaderHint.Type():
		return HandlerPrefixCancelHandover
	case HandoverMessageHeaderHint.Type():
		return HandlerPrefixHandoverMessage
	default:
		return nil
	}
}
