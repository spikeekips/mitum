package launch

import (
	"context"
	"io"
	"net"
	"net/http"
	netpprof "net/http/pprof"
	"net/url"
	"runtime/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

var (
	PprofRequestHeaderHint = hint.MustNewHint("pprof-header-v0.0.1")
	HandlerPrefixPprof     = "pprof"
)

type dummyResponseWriter struct {
	w io.Writer
}

func (dummyResponseWriter) Header() http.Header {
	return http.Header{}
}

func (w dummyResponseWriter) Write(b []byte) (int, error) {
	i, err := w.w.Write(b)
	if err != nil {
		return i, errors.Wrap(err, "")
	}

	return i, nil
}

func (dummyResponseWriter) WriteHeader(int) {
}

func NetworkHandlerPprofFunc(encs *encoder.Encoders) quicstream.Handler {
	return func(remote net.Addr, r io.Reader, w io.Writer) error {
		if !strings.HasPrefix(remote.String(), "127.") {
			return errors.Errorf("not allowed")
		}

		e := util.StringErrorFunc("failed pprof")

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*2) //nolint:gomnd //...
		defer cancel()

		enc, hb, err := isaacnetwork.HandlerReadHead(ctx, encs, r)
		if err != nil {
			return e(err, "")
		}

		var header PprofRequestHeader
		if err = encoder.Decode(enc, hb, &header); err != nil {
			return e(err, "")
		}

		if err = header.IsValid(nil); err != nil {
			return e(err, "")
		}

		req, err := http.NewRequest(http.MethodGet, "", nil)
		if err != nil {
			return e(err, "")
		}

		gc := "0"
		if header.GC() {
			gc = "1"
		}

		req.URL = &url.URL{
			Host: "localhost",
			RawQuery: url.Values{
				"seconds": []string{strconv.FormatUint(header.Seconds(), 10)},
				"gc":      []string{gc},
			}.Encode(),
		}

		res := isaacnetwork.NewResponseHeaderWithType(true, nil, isaac.NetworkResponseRawContentType)

		if err := isaacnetwork.Response(w, res, nil, enc); err != nil {
			return e(err, "")
		}

		netpprof.Handler(header.Label()).ServeHTTP(dummyResponseWriter{w: w}, req)

		return nil
	}
}

type PprofRequestHeader struct {
	label string
	isaacnetwork.BaseHeader
	seconds uint64
	gc      bool
}

func NewPprofRequestHeader(label string, seconds uint64, gc bool) PprofRequestHeader {
	return PprofRequestHeader{
		BaseHeader: isaacnetwork.NewBaseHeader(PprofRequestHeaderHint),
		label:      label,
		seconds:    seconds,
		gc:         gc,
	}
}

func (h PprofRequestHeader) IsValid([]byte) error {
	e := util.StringErrorFunc("invalid PprofRequestHeader")

	if err := h.BaseHinter.IsValid(PprofRequestHeaderHint.Type().Bytes()); err != nil {
		return e(err, "")
	}

	if len(h.label) < 1 {
		return e(nil, "empty profile label")
	}

	if h.seconds < 1 {
		return e(nil, "empty seconds")
	}

	if p := pprof.Lookup(h.label); p == nil {
		return e(nil, "unknown profile label, %q", h.label)
	}

	return nil
}

func (PprofRequestHeader) HandlerPrefix() string {
	return HandlerPrefixPprof
}

func (h PprofRequestHeader) Label() string {
	return h.label
}

func (h PprofRequestHeader) Seconds() uint64 {
	return h.seconds
}

func (h PprofRequestHeader) GC() bool {
	return h.gc
}

type pprofRequestHeaderJSONMarshaler struct {
	Label   string `json:"label"`
	Seconds uint64 `json:"seconds"`
	GC      bool   `json:"gc"`
}

func (h PprofRequestHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		pprofRequestHeaderJSONMarshaler
		isaacnetwork.BaseHeader
	}{
		BaseHeader: h.BaseHeader,
		pprofRequestHeaderJSONMarshaler: pprofRequestHeaderJSONMarshaler{
			Label:   h.label,
			Seconds: h.seconds,
			GC:      h.gc,
		},
	})
}

func (h *PprofRequestHeader) UnmarshalJSON(b []byte) error {
	e := util.StringErrorFunc("failed to unmarshal OperationHeader")

	var u pprofRequestHeaderJSONMarshaler

	if err := util.UnmarshalJSON(b, &u); err != nil {
		return e(err, "")
	}

	var uh hint.BaseHinter

	if err := util.UnmarshalJSON(b, &uh); err != nil {
		return err
	}

	h.BaseHeader = isaacnetwork.NewBaseHeader(uh.Hint())

	h.label = u.Label
	h.seconds = u.Seconds
	h.gc = u.GC

	return nil
}
