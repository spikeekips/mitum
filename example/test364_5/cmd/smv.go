package main

import (
	"encoding/json"
	"slices"

	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
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

type workerSMVsBundleJSONUnmarshaler struct {
	O uint64
	F valuehash.HashDecoder
	V []json.RawMessage
}

func (w *WorkerSMVsBundle) DecodeJSON(b []byte, enc encoder.Encoder) error {
	var u workerSMVsBundleJSONUnmarshaler

	if err := enc.Unmarshal(b, &u); err != nil {
		return err
	}

	w.V = make([]dop5.SMV, len(u.V))

	for i, v := range slices.All(u.V) {
		if err := encoder.Decode(enc, v, &w.V[i]); err != nil {
			return err
		}
	}

	w.O = u.O
	w.F = u.F.Hash()

	return nil
}
