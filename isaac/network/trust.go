package isaacnetwork

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/network"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
)

type TrustNodeChecker struct {
	enc    encoder.Encoder
	client isaac.NetworkClient
	*logging.Logging
	*util.ContextDaemon
	callback  func(called int64, _ []isaac.NodeConnInfo, _ error)
	cis       []interface{}
	local     base.Node
	networkID base.NetworkID
	interval  time.Duration
}

func NewTrustNodeChecker(
	local base.Node,
	networkID base.NetworkID,
	client isaac.NetworkClient,
	interval time.Duration,
	enc encoder.Encoder,
	cis []interface{},
	callback func(called int64, _ []isaac.NodeConnInfo, _ error),
) *TrustNodeChecker {
	c := &TrustNodeChecker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "node-conninfo-checker")
		}),
		local:     local,
		networkID: networkID,
		client:    client,
		interval:  interval,
		enc:       enc,
		cis:       cis,
		callback:  callback,
	}

	c.ContextDaemon = util.NewContextDaemon(c.start)

	return c
}

func (c *TrustNodeChecker) start(ctx context.Context) error {
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	var called int64 = -1

end:
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			called++

			if called < 1 {
				ticker.Reset(c.interval)
			}

			if len(c.cis) < 1 {
				c.callback(called, nil, errors.Errorf("empty conninfo"))

				continue end
			}

			ncis, err := c.check(ctx)

			c.callback(called, ncis, err)
		}
	}
}

func (c *TrustNodeChecker) check(ctx context.Context) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed to fetch NodeConnInfo")

	worker := util.NewDistributeWorker(ctx, int64(len(c.cis)), nil)
	defer worker.Close()

	nciss := make([][]isaac.NodeConnInfo, len(c.cis))

	go func() {
		defer worker.Done()

		for i := range c.cis {
			i := i
			ci := c.cis[i]

			if err := worker.NewJob(func(ctx context.Context, jobid uint64) error {
				ncis, err := c.fetch(ctx, ci)
				if err != nil {
					return err
				}

				nciss[i] = ncis

				return nil
			}); err != nil {
				return
			}
		}
	}()

	if err := worker.Wait(); err != nil {
		return nil, e(err, "")
	}

	var ncis []isaac.NodeConnInfo

	for i := range nciss {
		found := util.Filter2Slices(
			util.FilterSlices(nciss[i], func(a interface{}) bool {
				if a == nil {
					return false
				}

				aci := a.(NodeConnInfo) //nolint:forcetypeassert //...

				return !aci.Address().Equal(c.local.Address())
			}),
			ncis,
			func(a, b interface{}) bool {
				aci := a.(NodeConnInfo) //nolint:forcetypeassert //...
				bci := b.(NodeConnInfo) //nolint:forcetypeassert //...

				return aci.Address().Equal(bci.Address())
			})
		if len(found) < 1 {
			continue
		}

		dest := make([]isaac.NodeConnInfo, len(ncis)+len(found))
		copy(dest, ncis)

		for j := range found {
			dest[len(ncis)+j] = found[j].(NodeConnInfo) //nolint:forcetypeassert //...
		}

		ncis = dest
	}

	return ncis, nil
}

func (c *TrustNodeChecker) fetch(ctx context.Context, info interface{}) (ncis []isaac.NodeConnInfo, err error) {
	e := util.StringErrorFunc("failed to fetch NodeConnInfos")

	switch t := info.(type) {
	case NodeConnInfo:
		ncis = []isaac.NodeConnInfo{t}
	case quicstream.UDPConnInfo:
		ncis, err = c.client.SuffrageNodeConnInfo(ctx, t)
	case *url.URL:
		ncis, err = c.fetchFromURL(ctx, t)
	default:
		return nil, e(nil, "unsupported info, %v", info)
	}

	if err != nil {
		return nil, err
	}

	switch len(ncis) {
	case 0:
		return nil, nil
	case 1:
		if err := c.validate(ctx, ncis[0]); err != nil {
			if errors.Is(err, errIgnoreNodeconnInfo) {
				return nil, nil
			}

			return nil, e(err, "")
		}

		return ncis, nil
	}

	worker := util.NewErrgroupWorker(ctx, int64(len(ncis)))
	defer worker.Close()

	filtered := make([]isaac.NodeConnInfo, len(ncis))

	go func() {
		defer worker.Done()

		for i := range ncis {
			i := i
			nci := ncis[i]

			if err := worker.NewJob(func(ctx context.Context, _ uint64) error {
				switch err := c.validate(ctx, nci); {
				case err == nil:
				case errors.Is(err, errIgnoreNodeconnInfo):
				default:
					return err
				}

				filtered[i] = nci

				return nil
			}); err != nil {
				return
			}
		}
	}()

	if err := worker.Wait(); err != nil {
		return nil, e(err, "")
	}

	return filtered, nil
}

func (c *TrustNodeChecker) fetchFromURL(ctx context.Context, u *url.URL) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed to fetch NodeConnInfo from url, %q", u)

	httpclient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: network.HasTLSInsecure(u.Fragment),
			},
		},
	}

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return nil, nil // NOTE ignore network error
	}

	res, err := httpclient.Do(req)
	if err != nil {
		return nil, nil // NOTE ignore network error
	}

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, nil
	}

	defer func() {
		_ = res.Body.Close()
	}()

	if res.StatusCode != http.StatusOK {
		return nil, nil
	}

	var raw []json.RawMessage
	if err := c.enc.Unmarshal(b, &raw); err != nil {
		return nil, e(err, "")
	}

	ncis := make([]isaac.NodeConnInfo, len(raw))

	for i := range raw {
		if err := encoder.Decode(c.enc, raw[i], &ncis[i]); err != nil {
			return nil, e(err, "")
		}
	}

	return ncis, nil
}

var errIgnoreNodeconnInfo = util.NewError("ignore NodeConnInfo error")

func (c *TrustNodeChecker) validate(ctx context.Context, nci isaac.NodeConnInfo) error {
	e := util.StringErrorFunc("failed to fetch NodeConnInfo from node, %q", nci)

	if err := nci.IsValid(nil); err != nil {
		return e(err, "")
	}

	ci, err := nci.UDPConnInfo()

	var dnserr *net.DNSError

	switch {
	case err == nil:
	case errors.As(err, &dnserr):
		return errIgnoreNodeconnInfo.Wrap(err)
	}

	_, err = c.client.NodeChallenge(ctx, ci, c.networkID, nci.Address(), nci.Publickey(), util.UUID().Bytes())
	if errors.Is(err, base.SignatureVerificationError) {
		// NOTE if NodeChallenge failed, it means the node can not handle
		// it's online suffrage nodes properly. All NodeConnInfo of this
		// node is ignored.
		return e(err, "")
	}

	return nil
}
