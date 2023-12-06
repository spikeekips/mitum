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
	BlockItemRequestHeaderHint              = hint.MustNewHint("block-item-header-v0.0.1")
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
	BlockItemResponseHeaderHint             = hint.MustNewHint("block-item-response-header-v0.0.1")
)

var (
	HandlerPrefixRequestProposalString        = "request_proposal"
	HandlerPrefixProposalString               = "proposal"
	HandlerPrefixLastSuffrageProofString      = "last_suffrage_proof"
	HandlerPrefixSuffrageProofString          = "suffrage_proof"
	HandlerPrefixLastBlockMapString           = "last_blockmap"
	HandlerPrefixBlockMapString               = "blockmap"
	HandlerPrefixBlockItemString              = "block_item"
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

	HandlerPrefixRequestProposal        = quicstream.HashPrefix(HandlerPrefixRequestProposalString)
	HandlerPrefixProposal               = quicstream.HashPrefix(HandlerPrefixProposalString)
	HandlerPrefixLastSuffrageProof      = quicstream.HashPrefix(HandlerPrefixLastSuffrageProofString)
	HandlerPrefixSuffrageProof          = quicstream.HashPrefix(HandlerPrefixSuffrageProofString)
	HandlerPrefixLastBlockMap           = quicstream.HashPrefix(HandlerPrefixLastBlockMapString)
	HandlerPrefixBlockMap               = quicstream.HashPrefix(HandlerPrefixBlockMapString)
	HandlerPrefixBlockItem              = quicstream.HashPrefix(HandlerPrefixBlockItemString)
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
)

type BaseHeader struct {
	clientID string
	quicstreamheader.BaseRequestHeader
}

func NewBaseHeader(ht hint.Hint) BaseHeader {
	return BaseHeader{BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ht, headerPrefixByHint(ht))}
}

func (h BaseHeader) ClientID() string {
	return h.clientID
}

func (h *BaseHeader) SetClientID(id string) {
	h.clientID = id
}

type OperationRequestHeader struct {
	h util.Hash
	BaseHeader
}

func NewOperationRequestHeader(operationhash util.Hash) OperationRequestHeader {
	return OperationRequestHeader{
		BaseHeader: NewBaseHeader(OperationRequestHeaderHint),
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
	BaseHeader
}

func NewSendOperationRequestHeader() SendOperationRequestHeader {
	return SendOperationRequestHeader{
		BaseHeader: NewBaseHeader(SendOperationRequestHeaderHint),
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
	BaseHeader
	point base.Point
}

func NewRequestProposalRequestHeader(
	point base.Point,
	proposer base.Address,
	previousBlock util.Hash,
) RequestProposalRequestHeader {
	return RequestProposalRequestHeader{
		BaseHeader:    NewBaseHeader(RequestProposalRequestHeaderHint),
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
	BaseHeader
}

func NewProposalRequestHeader(proposal util.Hash) ProposalRequestHeader {
	return ProposalRequestHeader{
		BaseHeader: NewBaseHeader(ProposalRequestHeaderHint),
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
	BaseHeader
}

func NewLastSuffrageProofRequestHeader(state util.Hash) LastSuffrageProofRequestHeader {
	return LastSuffrageProofRequestHeader{
		BaseHeader: NewBaseHeader(LastSuffrageProofRequestHeaderHint),
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
	BaseHeader
	suffrageheight base.Height
}

func NewSuffrageProofRequestHeader(suffrageheight base.Height) SuffrageProofRequestHeader {
	return SuffrageProofRequestHeader{
		BaseHeader:     NewBaseHeader(SuffrageProofRequestHeaderHint),
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
	BaseHeader
}

func NewLastBlockMapRequestHeader(manifest util.Hash) LastBlockMapRequestHeader {
	return LastBlockMapRequestHeader{
		BaseHeader: NewBaseHeader(LastBlockMapRequestHeaderHint),
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
	BaseHeader
	height base.Height
}

func NewBlockMapRequestHeader(height base.Height) BlockMapRequestHeader {
	return BlockMapRequestHeader{
		BaseHeader: NewBaseHeader(BlockMapRequestHeaderHint),
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

type BlockItemRequestHeader struct {
	item base.BlockItemType
	BaseHeader
	height base.Height
}

func NewBlockItemRequestHeader(height base.Height, item base.BlockItemType) BlockItemRequestHeader {
	return BlockItemRequestHeader{
		BaseHeader: NewBaseHeader(BlockItemRequestHeaderHint),
		height:     height,
		item:       item,
	}
}

func (h BlockItemRequestHeader) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid BlockItemHeader")

	if err := h.BaseHinter.IsValid(BlockItemRequestHeaderHint.Type().Bytes()); err != nil {
		return e.Wrap(err)
	}

	if err := util.CheckIsValiders(nil, false, h.height, h.item); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (h BlockItemRequestHeader) Height() base.Height {
	return h.height
}

func (h BlockItemRequestHeader) Item() base.BlockItemType {
	return h.item
}

type NodeChallengeRequestHeader struct {
	me    base.Address
	mePub base.Publickey
	input []byte
	BaseHeader
}

func NewNodeChallengeRequestHeader(
	input []byte,
	me base.Address,
	mePub base.Publickey,
) NodeChallengeRequestHeader {
	return NodeChallengeRequestHeader{
		BaseHeader: NewBaseHeader(NodeChallengeRequestHeaderHint),
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
	BaseHeader
}

func NewSuffrageNodeConnInfoRequestHeader() SuffrageNodeConnInfoRequestHeader {
	return SuffrageNodeConnInfoRequestHeader{
		BaseHeader: NewBaseHeader(SuffrageNodeConnInfoRequestHeaderHint),
	}
}

func (h SuffrageNodeConnInfoRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SuffrageNodeConnInfoRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SuffrageNodeConnInfoHeader")
	}

	return nil
}

type SyncSourceConnInfoRequestHeader struct {
	BaseHeader
}

func NewSyncSourceConnInfoRequestHeader() SyncSourceConnInfoRequestHeader {
	return SyncSourceConnInfoRequestHeader{
		BaseHeader: NewBaseHeader(SyncSourceConnInfoRequestHeaderHint),
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
	BaseHeader
}

func NewStateRequestHeader(key string, h util.Hash) StateRequestHeader {
	return StateRequestHeader{
		BaseHeader: NewBaseHeader(StateRequestHeaderHint),
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
	BaseHeader
}

func NewExistsInStateOperationRequestHeader(facthash util.Hash) ExistsInStateOperationRequestHeader {
	return ExistsInStateOperationRequestHeader{
		BaseHeader: NewBaseHeader(ExistsInStateOperationRequestHeaderHint),
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
	BaseHeader
}

func NewNodeInfoRequestHeader() NodeInfoRequestHeader {
	return NodeInfoRequestHeader{
		BaseHeader: NewBaseHeader(NodeInfoRequestHeaderHint),
	}
}

func (h NodeInfoRequestHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(NodeInfoRequestHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid NodeInfoHeader")
	}

	return nil
}

type SendBallotsHeader struct {
	BaseHeader
}

func NewSendBallotsHeader() SendBallotsHeader {
	return SendBallotsHeader{
		BaseHeader: NewBaseHeader(SendBallotsHeaderHint),
	}
}

func (h SendBallotsHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(SendBallotsHeaderHint.Type().Bytes()); err != nil {
		return errors.WithMessage(err, "invalid SendBallotsHeader")
	}

	return nil
}

type SetAllowConsensusHeader struct {
	BaseHeader
	allow bool
}

func NewSetAllowConsensusHeader(allow bool) SetAllowConsensusHeader {
	return SetAllowConsensusHeader{
		BaseHeader: NewBaseHeader(SetAllowConsensusHeaderHint),
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
	BaseHeader
}

func NewStreamOperationsHeader(offset []byte) StreamOperationsHeader {
	return StreamOperationsHeader{
		BaseHeader: NewBaseHeader(StreamOperationsHeaderHint),
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

type aclUserHeader struct {
	acluser base.Publickey
}

func newACLUserHeader(acluser base.Publickey) aclUserHeader {
	return aclUserHeader{acluser: acluser}
}

func (h aclUserHeader) IsValid([]byte) error {
	return util.ErrInvalid.WithMessage(util.CheckIsValiders(nil, false, h.acluser), "acl user")
}

func (h aclUserHeader) ACLUser() base.Publickey {
	return h.acluser
}

type caHandoverHeader struct {
	connInfo quicstream.ConnInfo // conn info of X
	address  base.Address        // local address
	BaseHeader
}

func newCAHandoverHeader(ht hint.Hint, connInfo quicstream.ConnInfo, address base.Address) caHandoverHeader {
	return caHandoverHeader{
		BaseHeader: NewBaseHeader(ht),
		connInfo:   connInfo,
		address:    address,
	}
}

func (h caHandoverHeader) IsValid([]byte) error {
	if err := h.BaseHeader.IsValid(nil); err != nil {
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
	aclUserHeader
	caHandoverHeader
}

func NewStartHandoverHeader(
	connInfo quicstream.ConnInfo,
	address base.Address,
	acluser base.Publickey,
) StartHandoverHeader {
	return StartHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(StartHandoverHeaderHint, connInfo, address),
		aclUserHeader:    newACLUserHeader(acluser),
	}
}

func (h StartHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid StartHandoverHeader")
	}

	if err := h.aclUserHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid StartHandoverHeader")
	}

	return nil
}

type CheckHandoverHeader struct {
	aclUserHeader
	// connInfo quicstream.UDPConnInfo // conn info of y
	caHandoverHeader
}

func NewCheckHandoverHeader(
	connInfo quicstream.ConnInfo,
	address base.Address,
	acluser base.Publickey,
) CheckHandoverHeader {
	return CheckHandoverHeader{
		caHandoverHeader: newCAHandoverHeader(CheckHandoverHeaderHint, connInfo, address),
		aclUserHeader:    newACLUserHeader(acluser),
	}
}

func (h CheckHandoverHeader) IsValid([]byte) error {
	if err := h.caHandoverHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid CheckHandoverHeader")
	}

	if err := h.aclUserHeader.IsValid(nil); err != nil {
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
	aclUserHeader
	BaseHeader
}

func NewCancelHandoverHeader(acluser base.Publickey) CancelHandoverHeader {
	return CancelHandoverHeader{
		BaseHeader:    NewBaseHeader(CancelHandoverHeaderHint),
		aclUserHeader: newACLUserHeader(acluser),
	}
}

func (h CancelHandoverHeader) IsValid([]byte) error {
	if err := h.BaseHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid CancelHandoverHeader")
	}

	if err := h.aclUserHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid CancelHandoverHeader")
	}

	return nil
}

type HandoverMessageHeader struct {
	BaseHeader
}

func NewHandoverMessageHeader() HandoverMessageHeader {
	return HandoverMessageHeader{
		BaseHeader: NewBaseHeader(HandoverMessageHeaderHint),
	}
}

func (h HandoverMessageHeader) IsValid([]byte) error {
	if err := h.BaseHeader.IsValid(nil); err != nil {
		return util.ErrInvalid.WithMessage(err, "invalid HandoverMessageHeader")
	}

	return nil
}

// CheckHandoverXHeader checks only x node.
type CheckHandoverXHeader struct {
	address base.Address // local address
	BaseHeader
}

func NewCheckHandoverXHeader(address base.Address) CheckHandoverXHeader {
	return CheckHandoverXHeader{
		BaseHeader: NewBaseHeader(CheckHandoverXHeaderHint),
		address:    address,
	}
}

func (h CheckHandoverXHeader) IsValid([]byte) error {
	e := util.StringError("CheckHandoverXHeader")

	if err := h.BaseHeader.IsValid(nil); err != nil {
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

type BlockItemResponseHeader struct {
	compressFormat string
	quicstreamheader.BaseResponseHeader
}

func NewBlockItemResponseHeader(ok bool, err error, compressFormat string) BlockItemResponseHeader {
	return BlockItemResponseHeader{
		BaseResponseHeader: quicstreamheader.NewBaseResponseHeader(BlockItemResponseHeaderHint, ok, err),
		compressFormat:     compressFormat,
	}
}

func (h BlockItemResponseHeader) IsValid([]byte) error {
	return util.ErrInvalid.WithMessage(h.BaseResponseHeader.IsValid(nil), "BlockItemResponseHeader")
}

func (h BlockItemResponseHeader) CompressFormat() string {
	return h.compressFormat
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
	case BlockItemRequestHeaderHint.Type():
		return HandlerPrefixBlockItem
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
	default:
		return [32]byte{}
	}
	//revive:enable:cyclomatic
}
