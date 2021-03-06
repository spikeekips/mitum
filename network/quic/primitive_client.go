package quicnetwork

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/lucas-clemente/quic-go/http3"
	"golang.org/x/xerrors"

	"github.com/spikeekips/mitum/storage"
	"github.com/spikeekips/mitum/util/logging"
)

type QuicClient struct {
	*logging.Logging
	insecure   bool
	timeout    time.Duration
	retries    int
	quicConfig *quic.Config
}

func NewQuicClient(insecure bool, timeout time.Duration, retries int, quicConfig *quic.Config) (*QuicClient, error) {
	if timeout == 0 {
		timeout = time.Second * 3
	}
	if retries < 1 {
		retries = 1
	}

	if quicConfig == nil {
		quicConfig = &quic.Config{
			HandshakeTimeout: time.Second * 3, // long enough
			MaxIdleTimeout:   time.Second * 5,
		}
	}

	return &QuicClient{
		Logging: logging.NewLogging(func(c logging.Context) logging.Emitter {
			return c.Str("module", "network-quic-client")
		}),
		insecure:   insecure,
		timeout:    timeout,
		retries:    retries,
		quicConfig: quicConfig,
	}, nil
}

func (qc *QuicClient) newClient() (*http.Client, func() error /* close func */) {
	roundTripper := &http3.RoundTripper{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: qc.insecure, // nolint
		},
		QuicConfig: CloneConfig(qc.quicConfig),
	}

	return &http.Client{Transport: roundTripper}, roundTripper.Close
}

func (qc *QuicClient) Send(url string, b []byte, headers http.Header) error {
	l := qc.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Str("to", url).
			Int("content_length", len(b)).
			Str("request", "send")
	})

	var err error
	for i := 0; i < qc.retries; i++ {
		if err = qc.send(url, b, headers); err != nil {
			l.Warn().Err(err).Int("retries", i+1).Msg("failed to send; retries")
			continue
		}
		break
	}

	return err
}

func (qc *QuicClient) send(url string, b []byte, headers http.Header) error {
	l := qc.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Str("to", url).
			Int("content_length", len(b)).
			Str("request", "send")
	})

	var request *http.Request
	if req, err := http.NewRequest("POST", url, bytes.NewBuffer(b)); err != nil {
		l.Error().Err(err).Msg("failed to create request")
		return err
	} else {
		request = req
	}

	request.Header = headers

	ctx, cancel := context.WithTimeout(context.Background(), qc.timeout)
	defer cancel()

	client, closeFunc := qc.newClient()

	var response *http.Response
	if res, err := client.Do(request.WithContext(ctx)); err != nil {
		return err
	} else {
		response = res
	}

	defer func() {
		if err := closeFunc(); err != nil {
			l.Error().Err(err).Msg("failed to close")
		}
	}()

	defer func() {
		if err := response.Body.Close(); err != nil {
			l.Error().Err(err).Msg("failed to close response.Body")
		}
	}()

	return nil
}

func (qc *QuicClient) Request(url string, b []byte, headers http.Header) (QuicResponse, error) {
	l := qc.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Str("to", url).
			Int("content_length", len(b)).
			Str("request", "request")
	})

	var response QuicResponse
	var err error
	for i := 0; i < qc.retries; i++ {
		if response, err = qc.request(url, b, headers); err != nil {
			l.Error().Err(err).Int("retries", i+1).Msg("failed to request; retries")
			continue
		}
		break
	}

	return response, err
}

func (qc *QuicClient) request(url string, b []byte, headers http.Header) (QuicResponse, error) {
	l := qc.Log().WithLogger(func(ctx logging.Context) logging.Emitter {
		return ctx.Str("to", url).
			Int("content_length", len(b)).
			Str("request", "request")
	})

	var request *http.Request
	{
		var err error
		if b == nil {
			request, err = http.NewRequest("GET", url, nil)
		} else {
			request, err = http.NewRequest("POST", url, bytes.NewBuffer(b))
		}

		if err != nil {
			l.Error().Err(err).Msg("failed to create request")
			return QuicResponse{}, err
		}
	}

	request.Header = headers

	ctx, cancel := context.WithTimeout(context.Background(), qc.timeout)
	defer cancel()

	client, closeFunc := qc.newClient()

	var response *http.Response
	if res, err := client.Do(request.WithContext(ctx)); err != nil {
		l.Error().Err(err).Msgf("failed to send")
		return QuicResponse{}, err
	} else {
		l.Debug().
			Str("response", fmt.Sprintf("%v", res)).
			Msgf("got response")

		response = res
	}

	defer func() {
		if err := closeFunc(); err != nil {
			l.Error().Err(err).Msg("failed to close")
		} else {
			l.Debug().Msg("connection closed")
		}
	}()

	defer func() {
		if err := response.Body.Close(); err != nil {
			l.Error().Err(err).Msg("failed to close response.Body")
		}
	}()

	return NewQuicResponse(response)
}

type QuicResponse struct {
	status  int
	headers http.Header
	body    []byte
}

func NewQuicResponse(response *http.Response) (QuicResponse, error) {
	body := &bytes.Buffer{}
	if _, err := io.Copy(body, response.Body); err != nil {
		return QuicResponse{}, err
	}

	return QuicResponse{
		status:  response.StatusCode,
		headers: response.Header,
		body:    body.Bytes(),
	}, nil
}

func (qr QuicResponse) OK() bool {
	return qr.status == 200 || qr.status == 201
}

func (qr QuicResponse) Header() http.Header {
	return qr.headers
}

func (qr QuicResponse) Bytes() []byte {
	return qr.body
}

func (qr QuicResponse) Error() error {
	if qr.OK() {
		return nil
	} else if qr.status == http.StatusNotFound {
		return storage.NotFoundError.Errorf("failed to request: %s(%d)", qr.body, qr.status)
	}

	return xerrors.Errorf("failed to request: %s(%d)", qr.body, qr.status)
}

func CloneConfig(c *quic.Config) *quic.Config {
	cp := *c

	return &cp
}
