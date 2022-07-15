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
	"github.com/spikeekips/mitum/network/quicmemberlist"
	"github.com/spikeekips/mitum/network/quicstream"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/logging"
)

type SyncSourceType string

var (
	SyncSourceTypeNode          SyncSourceType = "sync-source-node"
	SyncSourceTypeSuffrageNodes SyncSourceType = "sync-source-suffrage-nodes"
	SyncSourceTypeSyncSources   SyncSourceType = "sync-source-sync-sources"
	SyncSourceTypeURL           SyncSourceType = "sync-source-url"
)

type SyncSource struct {
	Source interface{}
	Type   SyncSourceType
}

func (s SyncSource) IsValid([]byte) error {
	e := util.ErrInvalid.Errorf("invalid SyncSource")

	switch s.Type {
	case SyncSourceTypeNode,
		SyncSourceTypeSuffrageNodes,
		SyncSourceTypeSyncSources,
		SyncSourceTypeURL:
	default:
		return e.Errorf("unknown sync source type, %q", s.Type)
	}

	if s.Source == nil {
		return e.Errorf("unknown sync source type, %q", s.Type)
	}

	switch s.Source.(type) {
	case isaac.NodeConnInfo,
		quicstream.UDPConnInfo,
		quicmemberlist.NamedConnInfo,
		*url.URL:
	default:
		return e.Errorf("unsupported source, %T", s.Source)
	}

	switch t := s.Source.(type) {
	case isaac.NodeConnInfo:
		if s.Type != SyncSourceTypeNode && s.Type != SyncSourceTypeSuffrageNodes &&
			s.Type != SyncSourceTypeSyncSources {
			return e.Errorf("invalid type for NodeConnInfo, %v", s.Type)
		}
	case quicstream.UDPConnInfo:
		if s.Type != SyncSourceTypeSuffrageNodes && s.Type != SyncSourceTypeSyncSources {
			return e.Errorf("invalid type for UDPConnInfo, %v", s.Type)
		}
	case quicmemberlist.NamedConnInfo:
		if s.Type != SyncSourceTypeSuffrageNodes && s.Type != SyncSourceTypeSyncSources {
			return e.Errorf("invalid type for NamedConnInfo, %v", s.Type)
		}
	case *url.URL:
		if s.Type != SyncSourceTypeURL {
			return e.Errorf("invalid type for url, %v", s.Type)
		}

		if err := util.IsValidURL(t); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}

type SyncSourceChecker struct {
	enc    encoder.Encoder
	client isaac.NetworkClient
	*logging.Logging
	*util.ContextDaemon
	callback        func(called int64, _ []isaac.NodeConnInfo, _ error)
	sources         []SyncSource
	local           base.Node
	networkID       base.NetworkID
	interval        time.Duration
	validateTimeout time.Duration
}

func NewSyncSourceChecker(
	local base.Node,
	networkID base.NetworkID,
	client isaac.NetworkClient,
	interval time.Duration,
	enc encoder.Encoder,
	sources []SyncSource,
	callback func(called int64, _ []isaac.NodeConnInfo, _ error),
) *SyncSourceChecker {
	// NOTE SyncSources should be passed, IsValid()
	c := &SyncSourceChecker{
		Logging: logging.NewLogging(func(zctx zerolog.Context) zerolog.Context {
			return zctx.Str("module", "node-conninfo-checker")
		}),
		local:           local,
		networkID:       networkID,
		client:          client,
		interval:        interval,
		enc:             enc,
		sources:         sources,
		callback:        callback,
		validateTimeout: time.Second * 3, //nolint:gomnd //...
	}

	c.ContextDaemon = util.NewContextDaemon(c.start)

	return c
}

func (c *SyncSourceChecker) start(ctx context.Context) error {
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

			if len(c.sources) < 1 {
				c.callback(called, nil, errors.Errorf("empty SyncSource"))

				continue end
			}

			ncis, err := c.check(ctx)

			c.callback(called, ncis, err)
		}
	}
}

func (c *SyncSourceChecker) check(ctx context.Context) ([]isaac.NodeConnInfo, error) {
	e := util.StringErrorFunc("failed to fetch NodeConnInfo")

	worker := util.NewDistributeWorker(ctx, int64(len(c.sources)), nil)
	defer worker.Close()

	nciss := make([][]isaac.NodeConnInfo, len(c.sources))

	go func() {
		defer worker.Done()

		for i := range c.sources {
			i := i
			ci := c.sources[i]

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
		ss := nciss[i]

		found := util.Filter2Slices(
			util.FilterSlices(ss, func(_ interface{}, j int) bool {
				aci := ss[j]

				if aci == nil {
					return false
				}

				return !aci.Address().Equal(c.local.Address())
			}),
			ncis,
			func(a, _ interface{}, _, j int) bool {
				aci := a.(isaac.NodeConnInfo) //nolint:forcetypeassert //...

				return aci.Address().Equal(ncis[j].Address())
			})
		if len(found) < 1 {
			continue
		}

		dest := make([]isaac.NodeConnInfo, len(ncis)+len(found))
		copy(dest, ncis)

		for j := range found {
			dest[len(ncis)+j] = found[j].(isaac.NodeConnInfo) //nolint:forcetypeassert //...
		}

		ncis = dest
	}

	return ncis, nil
}

func (c *SyncSourceChecker) fetch(ctx context.Context, source SyncSource) (ncis []isaac.NodeConnInfo, err error) {
	e := util.StringErrorFunc("failed to fetch NodeConnInfos")

	switch source.Type {
	case SyncSourceTypeNode:
		ncis = []isaac.NodeConnInfo{source.Source.(isaac.NodeConnInfo)} //nolint:forcetypeassert //...
	case SyncSourceTypeSuffrageNodes:
		ncis, err = c.fetchFromSuffrageNodes(ctx, source.Source)
	case SyncSourceTypeSyncSources:
		ncis, err = c.fetchFromSyncSources(ctx, source.Source)
	case SyncSourceTypeURL:
		ncis, err = c.fetchFromURL(ctx, source.Source.(*url.URL))
	default:
		return nil, e(nil, "unsupported source type, %q", source.Type)
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

func (c *SyncSourceChecker) fetchFromSuffrageNodes(
	ctx context.Context, source interface{},
) (ncis []isaac.NodeConnInfo, _ error) {
	ncis, err := c.fetchNodeConnInfos(ctx, source, c.client.SuffrageNodeConnInfo)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to fetch suffrage nodes")
	}

	return ncis, nil
}

func (c *SyncSourceChecker) fetchFromSyncSources(
	ctx context.Context, source interface{},
) (ncis []isaac.NodeConnInfo, _ error) {
	ncis, err := c.fetchNodeConnInfos(ctx, source, c.client.SyncSourceConnInfo)
	if err != nil {
		return nil, errors.WithMessage(err, "failed to fetch sync sources")
	}

	return ncis, nil
}

func (c *SyncSourceChecker) fetchFromURL(ctx context.Context, u *url.URL) ([]isaac.NodeConnInfo, error) {
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

func (c *SyncSourceChecker) validate(ctx context.Context, nci isaac.NodeConnInfo) error {
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

	nctx, cancel := context.WithTimeout(ctx, c.validateTimeout)
	defer cancel()

	_, err = c.client.NodeChallenge(nctx, ci, c.networkID, nci.Address(), nci.Publickey(), util.UUID().Bytes())
	if errors.Is(err, base.SignatureVerificationError) {
		// NOTE if NodeChallenge failed, it means the node can not handle
		// it's online suffrage nodes properly. All NodeConnInfo of this
		// node is ignored.
		return e(err, "")
	}

	return nil
}

func (*SyncSourceChecker) fetchNodeConnInfos(
	ctx context.Context, source interface{},
	request func(context.Context, quicstream.UDPConnInfo) ([]isaac.NodeConnInfo, error),
) (ncis []isaac.NodeConnInfo, _ error) {
	var ci quicstream.UDPConnInfo

	switch t := source.(type) {
	case quicstream.UDPConnInfo:
		ci = t
	case isaac.NodeConnInfo, quicmemberlist.NamedConnInfo:
		i := t.(interface { //nolint:forcetypeassert //...
			UDPConnInfo() (quicstream.UDPConnInfo, error)
		})

		j, err := i.UDPConnInfo()
		if err != nil {
			return nil, err
		}

		ci = j
	default:
		return nil, errors.Errorf("unsupported source, %T", source)
	}

	ncis, err := request(ctx, ci)
	if err != nil {
		return nil, err
	}

	return ncis, nil
}