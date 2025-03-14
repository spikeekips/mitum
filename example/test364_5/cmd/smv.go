//go:generate go run -v -race ../tool/set_base_hinter.go - $GOFILE

package main

import (
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var WorkerSMVsBundleHint = hint.MustNewHint("worker-smvs-bundle-v0.0.1")

type WorkerSMVsBundle struct {
	O uint64
	F util.Hash
	V []dop5.SMV
}

type workerSMVsBundleMarshaler struct {
	hint.BaseHinter
	O interface{}
	F interface{}
	V interface{}
}

func newWorkerSMVsBundleMarshaller(v [3]interface{}) *workerSMVsBundleMarshaler {
	return &workerSMVsBundleMarshaler{
		BaseHinter: hint.NewBaseHinter(WorkerSMVsBundleHint),
		O:          v[0], F: v[1], V: v[2],
	}
}

func WorkerSMVsBundleDecodeJSON(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	var w WorkerSMVsBundle

	return w, dutil.FuncChain(
		dutil.AssignJSONInts(m["O"], &w.O),
		dutil.AssignJSONHash(m["F"], &w.F),
		encoder.DecodeSlice(enc, m["V"], &w.V),
	)
}
