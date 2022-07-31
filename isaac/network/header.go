package isaacnetwork

import (
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
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
)

var ResponseHeaderHint = hint.MustNewHint("response-header-v0.0.1")

type BaseHeader struct {
	prefix string
	hint.BaseHinter
}

func NewBaseHeader(ht hint.Hint) BaseHeader {
	return BaseHeader{
		BaseHinter: hint.NewBaseHinter(ht),
		prefix:     baseHeaderPrefixByHint(ht),
	}
}

func (h BaseHeader) HandlerPrefix() string {
	return h.prefix
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
	e := util.StringErrorFunc("invalid OperationHeader")

	if err := h.BaseHinter.IsValid(OperationRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.h); err != nil {
		return e(err, "")
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
	proposer base.Address
	BaseHeader
	point base.Point
}

func NewRequestProposalRequestHeader(point base.Point, proposer base.Address) RequestProposalRequestHeader {
	return RequestProposalRequestHeader{
		BaseHeader: NewBaseHeader(RequestProposalRequestHeaderHint),
		point:      point,
		proposer:   proposer,
	}
}

func (h RequestProposalRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid RequestProposalHeader")

	if err := h.BaseHinter.IsValid(RequestProposalRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.point, h.proposer); err != nil {
		return e(err, "")
	}

	return nil
}

func (h RequestProposalRequestHeader) Proposer() base.Address {
	return h.proposer
}

func (h RequestProposalRequestHeader) Point() base.Point {
	return h.point
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
	e := util.StringErrorFunc("invalid ProposalHeader")

	if err := h.BaseHinter.IsValid(ProposalRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.proposal); err != nil {
		return e(err, "")
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
	e := util.StringErrorFunc("invalid LastSuffrageProofHeader")

	if err := h.BaseHinter.IsValid(LastSuffrageProofRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if h.state != nil {
		if err := h.state.IsValid(nil); err != nil {
			return e(err, "")
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
	e := util.StringErrorFunc("invalid SuffrageProofHeader")

	if err := h.BaseHinter.IsValid(SuffrageProofRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.suffrageheight); err != nil {
		return e(err, "")
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
	e := util.StringErrorFunc("invalid LastLastBlockMapHeader")

	if err := h.BaseHinter.IsValid(LastBlockMapRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, true, h.manifest); err != nil {
		return e(err, "")
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
	e := util.StringErrorFunc("invalid LastBlockMapHeader")

	if err := h.BaseHinter.IsValid(BlockMapRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.height); err != nil {
		return e(err, "")
	}

	return nil
}

func (h BlockMapRequestHeader) Height() base.Height {
	return h.height
}

type BlockMapItemRequestHeader struct {
	item base.BlockMapItemType
	BaseHeader
	height base.Height
}

func NewBlockMapItemRequestHeader(height base.Height, item base.BlockMapItemType) BlockMapItemRequestHeader {
	return BlockMapItemRequestHeader{
		BaseHeader: NewBaseHeader(BlockMapItemRequestHeaderHint),
		height:     height,
		item:       item,
	}
}

func (h BlockMapItemRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid BlockMapItemHeader")

	if err := h.BaseHinter.IsValid(BlockMapItemRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if err := util.CheckIsValid(nil, false, h.height, h.item); err != nil {
		return e(err, "")
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
	input []byte
	BaseHeader
}

func NewNodeChallengeRequestHeader(input []byte) NodeChallengeRequestHeader {
	return NodeChallengeRequestHeader{
		BaseHeader: NewBaseHeader(NodeChallengeRequestHeaderHint),
		input:      input,
	}
}

func (h NodeChallengeRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid NodeChallengeHeader")

	if err := h.BaseHinter.IsValid(NodeChallengeRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if len(h.input) < 1 {
		return e(nil, "empty input")
	}

	return nil
}

func (h NodeChallengeRequestHeader) Input() []byte {
	return h.input
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
	e := util.StringErrorFunc("invalid stateHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if len(h.key) < 1 {
		return e(nil, "empty state key")
	}

	if h.h != nil {
		if err := h.h.IsValid(nil); err != nil {
			return e(err, "invalid state hash")
		}
	}

	return nil
}

func (StateRequestHeader) HandlerPrefix() string {
	return HandlerPrefixState
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
	e := util.StringErrorFunc("invalid existsInStateOperationHeader")

	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return e(err, "")
	}

	if h.facthash == nil {
		return e(nil, "empty operation fact hash")
	}

	if err := h.facthash.IsValid(nil); err != nil {
		return e(err, "")
	}

	return nil
}

func (ExistsInStateOperationRequestHeader) HandlerPrefix() string {
	return HandlerPrefixExistsInStateOperation
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

type ResponseHeader struct {
	ctype isaac.NetworkResponseContentType
	err   error
	BaseHeader
	ok bool
}

func NewResponseHeader(ok bool, err error) ResponseHeader {
	return ResponseHeader{
		BaseHeader: NewBaseHeader(ResponseHeaderHint),
		ok:         ok,
		err:        err,
	}
}

func NewResponseHeaderWithType(ok bool, err error, ctype isaac.NetworkResponseContentType) ResponseHeader {
	return ResponseHeader{
		BaseHeader: NewBaseHeader(ResponseHeaderHint),
		ok:         ok,
		err:        err,
		ctype:      ctype,
	}
}

func (r ResponseHeader) OK() bool {
	return r.ok
}

func (r ResponseHeader) Err() error {
	return r.err
}

func (r ResponseHeader) Type() isaac.NetworkResponseContentType {
	return r.ctype
}

func baseHeaderPrefixByHint(ht hint.Hint) string {
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
	default:
		return ""
	}
}
