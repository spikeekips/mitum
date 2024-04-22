package main

import (
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	WorkerCancelHeaderHint           = hint.MustNewHint("worker-cancel-header-v0.0.1")
	WorkerDoneHeaderHint             = hint.MustNewHint("worker-done-header-v0.0.1")
	WorkerReadyHeaderHint            = hint.MustNewHint("worker-ready-header-v0.0.1")
	WorkerNewOperationsHeaderHint    = hint.MustNewHint("worker-new-operations-header-v0.0.1")
	WorkerInsertOpHeaderHint         = hint.MustNewHint("worker-insert-op-header-v0.0.1")
	WorkerInsertValuesDoneHeaderHint = hint.MustNewHint("worker-insert-values-done-header-v0.0.1")
	WorkerPhaseDoneHeaderHint        = hint.MustNewHint("worker-phase-done-header-v0.0.1")
)

func handlerNameByHint(ht hint.Hint) quicstream.HandlerName {
	return quicstream.HandlerName("dop/" + ht.Type().String())
}

func headerPrefixByHint(ht hint.Hint) quicstream.HandlerPrefix {
	return quicstream.HashPrefix(handlerNameByHint(ht))
}

type BaseHeader struct {
	quicstreamheader.BaseRequestHeader
}

func NewBaseHeader(ht hint.Hint) BaseHeader {
	return BaseHeader{
		BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(ht, headerPrefixByHint(ht)),
	}
}

func (h *BaseHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.BaseRequestHeader); err != nil {
		return err
	}

	h.BaseRequestHeader = quicstreamheader.NewBaseRequestHeader(h.Hint(), headerPrefixByHint(h.Hint()))

	return nil
}

type BaseWorkerHeader struct {
	BaseHeader
	Worker string
	Height base.Height
}

func NewBaseWorkerHeader(ht hint.Hint, worker string, height base.Height) BaseWorkerHeader {
	return BaseWorkerHeader{
		BaseHeader: NewBaseHeader(ht),
		Worker:     worker,
		Height:     height,
	}
}

func (h BaseWorkerHeader) IsValid([]byte) error {
	if err := h.BaseHinter.IsValid(h.Hint().Type().Bytes()); err != nil {
		return err
	}

	return nil
}

type baseWorkerHeaderJSONMarshaler struct {
	Worker string
	Height base.Height
}

func (h BaseWorkerHeader) jsonMarshaler() baseWorkerHeaderJSONMarshaler {
	return baseWorkerHeaderJSONMarshaler{
		Worker: h.Worker,
		Height: h.Height,
	}
}

func (h BaseWorkerHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		baseWorkerHeaderJSONMarshaler
		quicstreamheader.BaseHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler:       h.BaseRequestHeader.JSONMarshaler(),
		baseWorkerHeaderJSONMarshaler: h.jsonMarshaler(),
	})
}

func (h *BaseWorkerHeader) UnmarshalJSON(b []byte) error {
	var u baseWorkerHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.Worker = u.Worker
	h.Height = u.Height

	return util.UnmarshalJSON(b, &h.BaseHeader)
}

type WorkerCancelHeader struct{ BaseWorkerHeader }

func NewWorkerCancelHeader(height base.Height) WorkerCancelHeader {
	return WorkerCancelHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerCancelHeaderHint, "manager", height),
	}
}

type WorkerDoneHeader struct{ BaseWorkerHeader }

func NewWorkerDoneHeader(height base.Height) WorkerDoneHeader {
	return WorkerDoneHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerDoneHeaderHint, "manager", height),
	}
}

type WorkerReadyHeader struct {
	BaseWorkerHeader
	Args processWorkerReadyArgs
}

func NewWorkerReadyHeader(args processWorkerReadyArgs) WorkerReadyHeader {
	return WorkerReadyHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerReadyHeaderHint, "manager", args.Height),
		Args:             args,
	}
}

type workerReadyHeaderJSONMarshaler struct {
	processWorkerReadyArgs
}

func (h WorkerReadyHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		quicstreamheader.BaseHeaderJSONMarshaler
		baseWorkerHeaderJSONMarshaler
		workerReadyHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler:       h.BaseRequestHeader.JSONMarshaler(),
		baseWorkerHeaderJSONMarshaler: h.jsonMarshaler(),
		workerReadyHeaderJSONMarshaler: workerReadyHeaderJSONMarshaler{
			processWorkerReadyArgs: h.Args,
		},
	})
}

func (h *WorkerReadyHeader) UnmarshalJSON(b []byte) error {
	if err := util.UnmarshalJSON(b, &h.Args); err != nil {
		return err
	}

	return util.UnmarshalJSON(b, &h.BaseWorkerHeader)
}

type WorkerNewOperationsHeader struct{ BaseWorkerHeader }

func NewWorkerNewOperationsHeader(height base.Height) WorkerNewOperationsHeader {
	return WorkerNewOperationsHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerNewOperationsHeaderHint, "manager", height),
	}
}

type WorkerInsertOpHeader struct{ BaseWorkerHeader }

func NewWorkerInsertOpHeader(worker string, height base.Height) WorkerInsertOpHeader {
	return WorkerInsertOpHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerInsertOpHeaderHint, worker, height),
	}
}

type WorkerInsertValuesDoneHeader struct{ BaseWorkerHeader }

func NewWorkerInsertValuesDoneHeader(worker string, height base.Height) WorkerInsertValuesDoneHeader {
	return WorkerInsertValuesDoneHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerInsertValuesDoneHeaderHint, worker, height),
	}
}

type WorkerPhaseDoneHeader struct {
	BaseWorkerHeader
	Phase int
}

func NewWorkerPhaseDoneHeader(worker string, height base.Height, phase int) WorkerPhaseDoneHeader {
	return WorkerPhaseDoneHeader{
		BaseWorkerHeader: NewBaseWorkerHeader(WorkerPhaseDoneHeaderHint, worker, height),
		Phase:            phase,
	}
}

type workerPhaseDoneHeaderJSONMarshaler struct {
	Phase int
}

func (h WorkerPhaseDoneHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		quicstreamheader.BaseHeaderJSONMarshaler
		baseWorkerHeaderJSONMarshaler
		workerPhaseDoneHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler:       h.BaseRequestHeader.JSONMarshaler(),
		baseWorkerHeaderJSONMarshaler: h.jsonMarshaler(),
		workerPhaseDoneHeaderJSONMarshaler: workerPhaseDoneHeaderJSONMarshaler{
			Phase: h.Phase,
		},
	})
}

func (h *WorkerPhaseDoneHeader) UnmarshalJSON(b []byte) error {
	var u workerPhaseDoneHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.Phase = u.Phase

	return util.UnmarshalJSON(b, &h.BaseWorkerHeader)
}
