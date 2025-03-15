package main

import (
	"bytes"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
	"go.uber.org/goleak"
)

var smvsBundleHint = hint.MustNewHint("worker-smvs-bundle-v0.0.1")

type smvsBundle struct {
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
		BaseHinter: hint.NewBaseHinter(smvsBundleHint),
		O:          v[0], F: v[1], V: v[2],
	}
}

func smvsBundleDecodeJSON(enc *jsonenc.Encoder, m map[string]any) (any, error) {
	var w smvsBundle

	return w, dutil.FuncChain(
		dutil.AssignJSONInts(m["O"], &w.O),
		dutil.AssignJSONHash(m["F"], &w.F),
		encoder.DecodeSlice(enc, m["V"], &w.V),
	)
}

func TestDecodeSMV(tt *testing.T) {
	defer goleak.VerifyNone(tt,
		goleak.IgnoreTopFunction("runtime/pprof.readProfile"))

	t := new(dutil.BaseSuite)
	t.SetT(tt)

	enc := jsonenc.NewEncoder(1 << 9)
	t.NoError(jsonenc.AddDecode(enc, base.DummyPublickeyHint, base.DummyPublickey{}, dop5.DecodeJSONDummyPublickey))
	t.NoError(jsonenc.AddDecode(enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON))
	t.NoError(jsonenc.AddDecode(enc, dop5.AccountSMVHint, &dop5.AccountSMV{}, dop5.AccountSMVDecodeJSON))
	t.NoError(jsonenc.AddDecode(enc, smvsBundleHint, smvsBundle{}, smvsBundleDecodeJSON))

	randomMergeValues := func(n int) []dop5.SMV {
		if n < 1 {
			return nil
		}

		s := make([]dop5.SMV, n)

		for i := range n {
			v, err := dop5.NewAccountSMV(base.NewDummyPublickey(util.UUID().String()), 33)
			t.IfErrPanic(err)

			s[i] = v
		}

		return s
	}

	writeBundle := func(w io.Writer, opindex uint64, v [2]any) error {
		b, err := enc.Marshal(newWorkerSMVsBundleMarshaller([3]any{opindex, v[0], v[1]}))
		if err != nil {
			return err
		}

		return frame.WriteFixed(w, b)
	}
	readBundle := func(r io.Reader) (uint64, [2]any, error) {
		var eof bool
		b, n, err := frame.ReadFixed(r)
		switch {
		case n < 1 && errors.Is(err, io.EOF):
			return 0, [2]any{}, err
		case errors.Is(err, io.EOF):
			eof = true
		case err != nil:
			return 0, [2]any{}, err
		}

		var bundle smvsBundle
		if err := encoder.Unmarshal(enc, b, &bundle); err != nil {
			return 0, [2]any{}, err
		}

		o := [2]any{bundle.F, bundle.V}
		if eof {
			return bundle.O, o, io.EOF
		}

		return bundle.O, o, nil
	}

	writeFrame := func(w io.Writer, opindex uint64, v [2]any) error {
		return writeSMVBundle(w, enc, opindex, v[0].(util.Hash), v[1].([]dop5.SMV))
	}
	readFrame := func(r io.Reader) (uint64, [2]any, error) {
		b, n, err := frame.ReadFixed(r)

		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			if n < 1 {
				return 0, [2]any{}, err
			}
		case err != nil:
			return 0, [2]any{}, err
		}

		opindex, fact, values, err := decodeSMVBundle(b, enc)
		if err != nil {
			return 0, [2]any{}, err
		}

		return opindex, [2]any{fact, values}, nil
	}

	do := func(n int, write func(io.Writer, uint64, [2]any) error, read func(io.Reader) (uint64, [2]any, error)) func() {
		return func() {
			ops := make([][2]any, n)
			for i := range ops {
				ops[i] = [2]any{
					valuehash.RandomSHA256(),
					randomMergeValues(3),
				}
			}

			buf := bytes.NewBuffer(nil)

			started := time.Now()
			for i, v := range slices.All(ops) {
				t.NoError(write(buf, uint64(i), v))
			}
			t.T().Log("marshal:", time.Since(started))

			rops := make([][2]any, n)

			started = time.Now()
			for {
				opindex, v, err := read(buf)
				if errors.Is(err, io.EOF) {
					break
				} else if err != nil {
					t.NoError(err)
				}

				rops[opindex] = v
			}
			t.T().Log("unmarshal:", time.Since(started))

			for opindex := range ops {
				a := ops[opindex]
				b := rops[opindex]

				t.True(a[0].(util.Hash).Equal(b[0].(util.Hash)))

				avs := a[1].([]dop5.SMV)
				bvs := b[1].([]dop5.SMV)
				t.Equal(len(avs), len(bvs))

				for i, v := range slices.All(avs) {
					av := v
					bv := bvs[i]

					t.Equal(av.Key(), bv.Key())
					t.True(base.IsEqualStateValue(av.Value(), bv.Value()))
				}
			}
		}
	}

	doBundle := func(n int) func() { return do(n, writeBundle, readBundle) }
	doFrame := func(n int) func() { return do(n, writeFrame, readFrame) }

	t.Run("3333", func() {
		t.Run("bundle", doBundle(3333))
		t.Run("frame", doFrame(3333))
	})

	t.Run("6666", func() {
		t.Run("bundle", doBundle(6666))
		t.Run("frame", doFrame(6666))
	})

	t.Run("33333", func() {
		t.Run("bundle", doBundle(33333))
		t.Run("frame", doFrame(33333))
	})
}
