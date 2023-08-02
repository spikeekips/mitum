package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	OperationRequestHeaderHint              = hint.MustNewHint("operation-header-v0.0.1")
	SendOperationRequestHeaderHint          = hint.MustNewHint("send-operation-header-v0.0.1")
	RequestProposalRequestHeaderHint        = hint.MustNewHint("request-proposal-header-v0.0.1")
	ProposalRequestHeaderHint               = hint.MustNewHint("proposal-header-v0.0.1")
	LastSuffrageProofRequestHeaderHint      = hint.MustNewHint("last-suffrage-proof-header-v0.0.1")
	SuffrageProofRequestHeaderHint          = hint.MustNewHint("suffrage-proof-header-v0.0.1")
	LastBlockMapRequestHeaderHint           = hint.MustNewHint("last-blockmap-header-v0.0.1")
	BlockMapRequestHeaderHint               = hint.MustNewHint("blockmap-header-v0.0.1")
	BlockMapItemRequestHeaderHint           = hint.MustNewHint("blockmap-item-header-v0.0.1")
	NodeChallengeRequestHeaderHint          = hint.MustNewHint("node-challenge-header-v0.0.1")
	SuffrageNodeConnInfoRequestHeaderHint   = hint.MustNewHint("suffrage-node-conninfo-header-v0.0.1")
	SyncSourceConnInfoRequestHeaderHint     = hint.MustNewHint("sync-source-conninfo-header-v0.0.1")
	StateRequestHeaderHint                  = hint.MustNewHint("state-header-v0.0.1")
	ExistsInStateOperationRequestHeaderHint = hint.MustNewHint("exists-instate-operation-header-v0.0.1")
	NodeInfoRequestHeaderHint               = hint.MustNewHint("node-info-header-v0.0.1")
	SendBallotsHeaderHint                   = hint.MustNewHint("send-ballots-header-v0.0.1")
	SetAllowConsensusHeaderHint             = hint.MustNewHint("set-allow-consensus-header-v0.0.1")
	StreamOperationsHeaderHint              = hint.MustNewHint("stream-operations-header-v0.0.1")
	StartHandoverHeaderHint                 = hint.MustNewHint("start-handover-header-v0.0.1")
	CheckHandoverHeaderHint                 = hint.MustNewHint("check-handover-header-v0.0.1")
	AskHandoverHeaderHint                   = hint.MustNewHint("ask-handover-header-v0.0.1")
	AskHandoverResponseHeaderHint           = hint.MustNewHint("ask-handover-response-header-v0.0.1")
	CancelHandoverHeaderHint                = hint.MustNewHint("cancel-handover-header-v0.0.1")
	HandoverMessageHeaderHint               = hint.MustNewHint("handover-message-header-v0.0.1")
	CheckHandoverXHeaderHint                = hint.MustNewHint("check-handover-x-header-v0.0.1")
	LastHandoverYLogsHeaderHint             = hint.MustNewHint("last-handover-y-logs-header-v0.0.1")
)

var (
	HandlerPrefixRequestProposalString        = "request_proposal"
	HandlerPrefixProposalString               = "proposal"
	HandlerPrefixLastSuffrageProofString      = "last_suffrage_proof"
	HandlerPrefixSuffrageProofString          = "suffrage_proof"
	HandlerPrefixLastBlockMapString           = "last_blockmap"
	HandlerPrefixBlockMapString               = "blockmap"
	HandlerPrefixBlockMapItemString           = "blockmap_item"
	HandlerPrefixMemberlistString             = "memberlist"
	HandlerPrefixNodeChallengeString          = "node_challenge"
	HandlerPrefixSuffrageNodeConnInfoString   = "suffrage_node_conninfo"
	HandlerPrefixSyncSourceConnInfoString     = "sync_source_conninfo"
	HandlerPrefixOperationString              = "operation"
	HandlerPrefixSendOperationString          = "send_operation"
	HandlerPrefixStateString                  = "state"
	HandlerPrefixExistsInStateOperationString = "exists_instate_operation"
	HandlerPrefixNodeInfoString               = "node_info"
	HandlerPrefixSendBallotsString            = "send_ballots"
	HandlerPrefixSetAllowConsensusString      = "set_allow_consensus"
	HandlerPrefixStreamOperationsString       = "stream_operations"
	HandlerPrefixStartHandoverString          = "start_handover"
	HandlerPrefixCheckHandoverString          = "check_handover"
	HandlerPrefixAskHandoverString            = "ask_handover"
	HandlerPrefixCancelHandoverString         = "cancel_handover"
	HandlerPrefixHandoverMessageString        = "handover_message"
	HandlerPrefixCheckHandoverXString         = "check_handover_x"
	HandlerPrefixLastHandoverYLogsString      = "last_handover_y_logs"

	HandlerPrefixRequestProposal        = quicstream.HashPrefix(HandlerPrefixRequestProposalString)
	HandlerPrefixProposal               = quicstream.HashPrefix(HandlerPrefixProposalString)
	HandlerPrefixLastSuffrageProof      = quicstream.HashPrefix(HandlerPrefixLastSuffrageProofString)
	HandlerPrefixSuffrageProof          = quicstream.HashPrefix(HandlerPrefixSuffrageProofString)
	HandlerPrefixLastBlockMap           = quicstream.HashPrefix(HandlerPrefixLastBlockMapString)
	HandlerPrefixBlockMap               = quicstream.HashPrefix(HandlerPrefixBlockMapString)
	HandlerPrefixBlockMapItem           = quicstream.HashPrefix(HandlerPrefixBlockMapItemString)
	HandlerPrefixMemberlist             = quicstream.HashPrefix(HandlerPrefixMemberlistString)
	HandlerPrefixNodeChallenge          = quicstream.HashPrefix(HandlerPrefixNodeChallengeString)
	HandlerPrefixSuffrageNodeConnInfo   = quicstream.HashPrefix(HandlerPrefixSuffrageNodeConnInfoString)
	HandlerPrefixSyncSourceConnInfo     = quicstream.HashPrefix(HandlerPrefixSyncSourceConnInfoString)
	HandlerPrefixOperation              = quicstream.HashPrefix(HandlerPrefixOperationString)
	HandlerPrefixSendOperation          = quicstream.HashPrefix(HandlerPrefixSendOperationString)
	HandlerPrefixState                  = quicstream.HashPrefix(HandlerPrefixStateString)
	HandlerPrefixExistsInStateOperation = quicstream.HashPrefix(HandlerPrefixExistsInStateOperationString)
	HandlerPrefixNodeInfo               = quicstream.HashPrefix(HandlerPrefixNodeInfoString)
	HandlerPrefixSendBallots            = quicstream.HashPrefix(HandlerPrefixSendBallotsString)
	HandlerPrefixSetAllowConsensus      = quicstream.HashPrefix(HandlerPrefixSetAllowConsensusString)
	HandlerPrefixStreamOperations       = quicstream.HashPrefix(HandlerPrefixStreamOperationsString)
	HandlerPrefixStartHandover          = quicstream.HashPrefix(HandlerPrefixStartHandoverString)
	HandlerPrefixCheckHandover          = quicstream.HashPrefix(HandlerPrefixCheckHandoverString)
	HandlerPrefixAskHandover            = quicstream.HashPrefix(HandlerPrefixAskHandoverString)
	HandlerPrefixCancelHandover         = quicstream.HashPrefix(HandlerPrefixCancelHandoverString)
	HandlerPrefixHandoverMessage        = quicstream.HashPrefix(HandlerPrefixHandoverMessageString)
	HandlerPrefixCheckHandoverX         = quicstream.HashPrefix(HandlerPrefixCheckHandoverXString)
	HandlerPrefixLastHandoverYLogs      = quicstream.HashPrefix(HandlerPrefixLastHandoverYLogsString)
)

type baseHeader struct {
	quicstreamheader.BaseRequestHeader
}

func newBaseHeader(ht hint.Hint) baseHeader {
	return baseHeader{BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ht, headerPrefixByHint(ht))}
}

type OperationRequestHeader struct {
	h util.Hash
	baseHeader
}

func NewOperationRequestHeader(operationhash util.Hash) OperationRequestHeader {
	return OperationRequestHeader{
		baseHeader: newBaseHeader(OperationRequestHeaderHint),
		h:          operationhash,
	}
}

func (h OperationRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid OperationRequestHeader")

	if err := h.BaseHinter.IsValid(OperationRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.h); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h OperationRequestHeader) Operation() util.Hash {
	return h.h
}

type SendOperationRequestHeader struct {
	baseHeader
}

func NewSendOperationRequestHeader() SendOperationRequestHeader {
	return SendOperationRequestHeader{
		baseHeader: newBaseHeader(SendOperationRequestHeaderHint),
	}
}

func (h SendOperationRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SendOperationRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SendOperationHeader")
	}

	return nil
}

type RequestProposalRequestHeader struct {
	previousBlock util.Hash
	proposer      base.Address
	baseHeader
	point base.Point
}

func NewRequestProposalRequestHeader(
	point base.Point,
	proposer base.Address,
	previousBlock util.Hash,
) RequestProposalRequestHeader {
	return RequestProposalRequestHeader{
		baseHeader:    newBaseHeader(RequestProposalRequestHeaderHint),
		point:         point,
		proposer:      proposer,
		previousBlock: previousBlock,
	}
}

func (h RequestProposalRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid RequestProposalHeader")

	if err := h.BaseHinter.IsValid(RequestProposalRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.point, h.proposer, h.previousBlock); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h RequestProposalRequestHeader) Proposer() base.Address {
	return h.proposer
}

func (h RequestProposalRequestHeader) Point() base.Point {
	return h.point
}

func (h RequestProposalRequestHeader) PreviousBlock() util.Hash {
	return h.previousBlock
}

type ProposalRequestHeader struct {
	proposal util.Hash
	baseHeader
}

func NewProposalRequestHeader(proposal util.Hash) ProposalRequestHeader {
	return ProposalRequestHeader{
		baseHeader: newBaseHeader(ProposalRequestHeaderHint),
		proposal:   proposal,
	}
}

func (h ProposalRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid ProposalHeader")

	if err := h.BaseHinter.IsValid(ProposalRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.proposal); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h ProposalRequestHeader) Proposal() util.Hash {
	return h.proposal
}

type LastSuffrageProofRequestHeader struct {
	state util.Hash
	baseHeader
}

func NewLastSuffrageProofRequestHeader(state util.Hash) LastSuffrageProofRequestHeader {
	return LastSuffrageProofRequestHeader{
		baseHeader: newBaseHeader(LastSuffrageProofRequestHeaderHint),
		state:      state,
	}
}

func (h LastSuffrageProofRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid LastSuffrageProofHeader")

	if err := h.BaseHinter.IsValid(LastSuffrageProofRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if h.state != nil {
		if err := h.state.IsValid(nil); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h LastSuffrageProofRequestHeader) State() util.Hash {
	return h.state
}

type SuffrageProofRequestHeader struct {
	baseHeader
	suffrageheight base.Height
}

func NewSuffrageProofRequestHeader(suffrageheight base.Height) SuffrageProofRequestHeader {
	return SuffrageProofRequestHeader{
		baseHeader:     newBaseHeader(SuffrageProofRequestHeaderHint),
		suffrageheight: suffrageheight,
	}
}

func (h SuffrageProofRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SuffrageProofHeader")

	if err := h.BaseHinter.IsValid(SuffrageProofRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.suffrageheight); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h SuffrageProofRequestHeader) Height() base.Height {
	return h.suffrageheight
}

type LastBlockMapRequestHeader struct {
	manifest util.Hash
	baseHeader
}

func NewLastBlockMapRequestHeader(manifest util.Hash) LastBlockMapRequestHeader {
	return LastBlockMapRequestHeader{
		baseHeader: newBaseHeader(LastBlockMapRequestHeaderHint),
		manifest:   manifest,
	}
}

func (h LastBlockMapRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid LastLastBlockMapHeader")

	if err := h.BaseHinter.IsValid(LastBlockMapRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, true, h.manifest); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h LastBlockMapRequestHeader) Manifest() util.Hash {
	return h.manifest
}

type BlockMapRequestHeader struct {
	baseHeader
	height base.Height
}

func NewBlockMapRequestHeader(height base.Height) BlockMapRequestHeader {
	return BlockMapRequestHeader{
		baseHeader: newBaseHeader(BlockMapRequestHeaderHint),
		height:     height,
	}
}

func (h BlockMapRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid LastBlockMapHeader")

	if err := h.BaseHinter.IsValid(BlockMapRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.height); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h BlockMapRequestHeader) Height() base.Height {
	return h.height
}

type BlockMapItemRequestHeader struct {
	item base.BlockMapItemType
	baseHeader
	height base.Height
}

func NewBlockMapItemRequestHeader(height base.Height, item base.BlockMapItemType) BlockMapItemRequestHeader {
	return BlockMapItemRequestHeader{
		baseHeader: newBaseHeader(BlockMapItemRequestHeaderHint),
		height:     height,
		item:       item,
	}
}

func (h BlockMapItemRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BlockMapItemHeader")

	if err := h.BaseHinter.IsValid(BlockMapItemRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.height, h.item); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h BlockMapItemRequestHeader) Height() base.Height {
	return h.height
}

func (h BlockMapItemRequestHeader) Item() base.BlockMapItemType {
	return h.item
}

type NodeChallengeRequestHeader struct {
	me    base.Address
	mePub base.Publickey
	input []byte
	baseHeader
}

func NewNodeChallengeRequestHeader(
	input []byte,
	me base.Address,
	mePub base.Publickey,
) NodeChallengeRequestHeader {
	return NodeChallengeRequestHeader{
		baseHeader: newBaseHeader(NodeChallengeRequestHeaderHint),
		input:      input,
		me:         me,
		mePub:      mePub,
	}
}

func (h NodeChallengeRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid NodeChallengeHeader")

	if err := h.BaseHinter.IsValid(NodeChallengeRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if len(h.input) < 1 {
		return e.Errorf("empty input")
	}

	switch {
	case h.me == nil && h.mePub == nil:
	case h.me == nil || h.mePub == nil:
		return e.Errorf("me or me pub missing")
	default:
		if err := util.CheckIsValiders(nil, true, h.me, h.mePub); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

func (h NodeChallengeRequestHeader) Input() []byte {
	return h.input
}

func (h NodeChallengeRequestHeader) Me() base.Address {
	return h.me
}

func (h NodeChallengeRequestHeader) MePublickey() base.Publickey {
	return h.mePub
}

type SuffrageNodeConnInfoRequestHeader struct {
	baseHeader
}

func NewSuffrageNodeConnInfoRequestHeader() SuffrageNodeConnInfoRequestHeader {
	return SuffrageNodeConnInfoRequestHeader{
		baseHeader: newBaseHeader(SuffrageNodeConnInfoRequestHeaderHint),
	}
}

func (h SuffrageNodeConnInfoRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SuffrageNodeConnInfoRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SuffrageNodeConnInfoHeader")
	}

	return nil
}

type SyncSourceConnInfoRequestHeader struct {
	baseHeader
}

func NewSyncSourceConnInfoRequestHeader() SyncSourceConnInfoRequestHeader {
	return SyncSourceConnInfoRequestHeader{
		baseHeader: newBaseHeader(SyncSourceConnInfoRequestHeaderHint),
	}
}

func (h SyncSourceConnInfoRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SyncSourceConnInfoRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SyncSourceConnInfoHeader")
	}

	return nil
}

type StateRequestHeader struct {
	key string
	h   util.Hash
	baseHeader
}

func NewStateRequestHeader(key string, h util.Hash) StateRequestHeader {
	return StateRequestHeader{
		baseHeader: newBaseHeader(StateRequestHeaderHint),
		key:        key,
		h:          h,
	}
}

func (h StateRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid stateHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if len(h.key) < 1 {
		return e.Errorf("empty state key")
	}

	if h.h != nil {
		if err := h.h.IsValid(nil); err != nil {
			return e.WithMessage(err, "invalid state hash")
		}
	}

	return nil
}

func (h StateRequestHeader) Key() string {
	return h.key
}

func (h StateRequestHeader) Hash() util.Hash {
	return h.h
}

type ExistsInStateOperationRequestHeader struct {
	facthash util.Hash
	baseHeader
}

func NewExistsInStateOperationRequestHeader(facthash util.Hash) ExistsInStateOperationRequestHeader {
	return ExistsInStateOperationRequestHeader{
		baseHeader: newBaseHeader(ExistsInStateOperationRequestHeaderHint),
		facthash:   facthash,
	}
}

func (h ExistsInStateOperationRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid existsInStateOperationHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if h.facthash == nil {
		return e.Errorf("empty operation fact hash")
	}

	if err := h.facthash.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h ExistsInStateOperationRequestHeader) FactHash() util.Hash {
	return h.facthash
}

type NodeInfoRequestHeader struct {
	baseHeader
}

func NewNodeInfoRequestHeader() NodeInfoRequestHeader {
	return NodeInfoRequestHeader{
		baseHeader: newBaseHeader(NodeInfoRequestHeaderHint),
	}
}

func (h NodeInfoRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(NodeInfoRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid NodeInfoHeader")
	}

	return nil
}

type SendBallotsHeader struct {
	baseHeader
}

func NewSendBallotsHeader() SendBallotsHeader {
	return SendBallotsHeader{
		baseHeader: newBaseHeader(SendBallotsHeaderHint),
	}
}

func (h SendBallotsHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SendBallotsHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SendBallotsHeader")
	}

	return nil
}

type SetAllowConsensusHeader struct {
	baseHeader
	allow bool
}

func NewSetAllowConsensusHeader(allow bool) SetAllowConsensusHeader {
	return SetAllowConsensusHeader{
		baseHeader: newBaseHeader(SetAllowConsensusHeaderHint),
		allow:      allow,
	}
}

func (h SetAllowConsensusHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SetAllowConsensusHeaderHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid SetAllowConsensusHeader")
	}

	return nil
}

func (h SetAllowConsensusHeader) Allow() bool {
	return h.allow
}

type StreamOperationsHeader struct {
	offset []byte
	baseHeader
}

func NewStreamOperationsHeader(offset []byte) StreamOperationsHeader {
	return StreamOperationsHeader{
		baseHeader: newBaseHeader(StreamOperationsHeaderHint),
		offset:     offset,
	}
}

func (h StreamOperationsHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(StreamOperationsHeaderHint.Type().Bytes()); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid StreamOperationsHeader")
	}

	return nil
}

func (h StreamOperationsHeader) Offset() []byte {
	return h.offset
}

type caHandoverHeader struct {
	connInfo quicstream.ConnInfo // conn info of X
	address  base.Address        // local address
	baseHeader
}

func newCAHandoverHeader(ht hint.Hint, connInfo quicstream.ConnInfo, address base.Address) caHandoverHeader {
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

func (h caHandoverHeader) ConnInfo() quicstream.ConnInfo {
	return h.connInfo
}

func (h caHandoverHeader) Address() base.Address {
	return h.address
}

type StartHandoverHeader struct {
	caHandoverHeader
}

func NewStartHandoverHeader(connInfo quicstream.ConnInfo, address base.Address) StartHandoverHeader {
	return StartHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(StartHandoverHeaderHint, connInfo, address),
	}
}

func (h StartHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid StartHandoverHeader")
	}

	return nil
}

type CheckHandoverHeader struct {
	// connInfo quicstream.UDPConnInfo // conn info of y
	caHandoverHeader
}

func NewCheckHandoverHeader(connInfo quicstream.ConnInfo, address base.Address) CheckHandoverHeader {
	return CheckHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(CheckHandoverHeaderHint, connInfo, address),
	}
}

func (h CheckHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid CheckHandoverHeader")
	}

	return nil
}

type AskHandoverHeader struct {
	caHandoverHeader
}

func NewAskHandoverHeader(connInfo quicstream.ConnInfo, address base.Address) AskHandoverHeader {
	return AskHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(AskHandoverHeaderHint, connInfo, address),
	}
}

func (h AskHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid AskHandoverHeader")
	}

	return nil
}

// AskHandoverResponseHeader has handover id. If OK() is true, y broker can move
// to consensus without handover process.
type AskHandoverResponseHeader struct {
	id string // id is broker ID
	quicstreamheader.BaseResponseHeader
}

func NewAskHandoverResponseHeader(ok bool, err error, id string) AskHandoverResponseHeader {
	return AskHandoverResponseHeader{
		BaseResponseHeader: quicstreamheader.NewBaseResponseHeader(AskHandoverResponseHeaderHint, ok, err),
		id:                 id,
	}
}

func (h AskHandoverResponseHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("AskHandoverResponseHeader")

	if err := h.BaseResponseHeader.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	switch {
	case h.Err() != nil:
	case !h.OK():
	case len(h.id) < 1:
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
		return util.ErrInvalid.WithMessage(err, "invalid CancelHandoverHeader")
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
		return util.ErrInvalid.WithMessage(err, "invalid HandoverMessageHeader")
	}

	return nil
}

// CheckHandoverXHeader checks only x node.
type CheckHandoverXHeader struct {
	address base.Address // local address
	baseHeader
}

func NewCheckHandoverXHeader(address base.Address) CheckHandoverXHeader {
	return CheckHandoverXHeader{
		baseHeader: newBaseHeader(CheckHandoverXHeaderHint),
		address:    address,
	}
}

func (h CheckHandoverXHeader) IsValid([]byte) error {
	e := util.StringError("CheckHandoverXHeader")

	if err := h.baseHeader.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	if err := h.address.IsValid(nil); err != nil {
		return e.WithMessage(err, "address")
	}

	return nil
}

func (h CheckHandoverXHeader) Address() base.Address {
	return h.address
}

type LastHandoverYLogsHeader struct {
	baseHeader
}

func NewLastHandoverYLogsHeader() LastHandoverYLogsHeader {
	return LastHandoverYLogsHeader{
		baseHeader: newBaseHeader(LastHandoverYLogsHeaderHint),
	}
}

func (h LastHandoverYLogsHeader) IsValid([]byte) error {
	e := util.StringError("LastHandoverYLogsHeader")

	if err := h.baseHeader.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	return nil
}

//revive:disable:cyclomatic

func headerPrefixByHint(ht hint.Hint) [32]byte {
	switch ht.Type() {
	case RequestProposalRequestHeaderHint.Type():
		return HandlerPrefixRequestProposal
	case ProposalRequestHeaderHint.Type():
		return HandlerPrefixProposal
	case LastSuffrageProofRequestHeaderHint.Type():
		return HandlerPrefixLastSuffrageProof
	case SuffrageProofRequestHeaderHint.Type():
		return HandlerPrefixSuffrageProof
	case LastBlockMapRequestHeaderHint.Type():
		return HandlerPrefixLastBlockMap
	case BlockMapRequestHeaderHint.Type():
		return HandlerPrefixBlockMap
	case BlockMapItemRequestHeaderHint.Type():
		return HandlerPrefixBlockMapItem
	case NodeChallengeRequestHeaderHint.Type():
		return HandlerPrefixNodeChallenge
	case SuffrageNodeConnInfoRequestHeaderHint.Type():
		return HandlerPrefixSuffrageNodeConnInfo
	case SyncSourceConnInfoRequestHeaderHint.Type():
		return HandlerPrefixSyncSourceConnInfo
	case OperationRequestHeaderHint.Type():
		return HandlerPrefixOperation
	case SendOperationRequestHeaderHint.Type():
		return HandlerPrefixSendOperation
	case StateRequestHeaderHint.Type():
		return HandlerPrefixState
	case ExistsInStateOperationRequestHeaderHint.Type():
		return HandlerPrefixExistsInStateOperation
	case NodeInfoRequestHeaderHint.Type():
		return HandlerPrefixNodeInfo
	case SendBallotsHeaderHint.Type():
		return HandlerPrefixSendBallots
	case SetAllowConsensusHeaderHint.Type():
		return HandlerPrefixSetAllowConsensus
	case StreamOperationsHeaderHint.Type():
		return HandlerPrefixStreamOperations
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
	case CheckHandoverXHeaderHint.Type():
		return HandlerPrefixCheckHandoverX
	case LastHandoverYLogsHeaderHint.Type():
		return HandlerPrefixLastHandoverYLogs
	default:
		return [32]byte{}
	}
}

//revive:enable:cyclomatic
