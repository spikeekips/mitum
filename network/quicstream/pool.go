package quicstream

import (
	"context"
	"io"
	"math"
	"net"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type PoolClient struct {
	clients  *util.ShardedMap
	onerrorf func(addr *net.UDPAddr, c *Client, err error)
}

func NewPoolClient() *PoolClient {
	return &PoolClient{
		clients: util.NewShardedMap(math.MaxInt8),
	}
}

func (p *PoolClient) Close() error {
	e := util.StringErrorFunc("failed to close PoolClient")

	defer p.clients.Close()

	switch {
	case p.clients.Len() < 1:
		return nil
	case p.clients.Len() < 2: //nolint:gomnd //...
		var err error

		p.clients.Traverse(func(_, i interface{}) bool {
			item := i.(*poolClientItem) //nolint:forcetypeassert //...
			err = item.client.Close()

			return true
		})

		if err != nil {
			return e(err, "")
		}

		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), int64(p.clients.Len()))
	defer worker.Close()

	var cerr error

	p.clients.Traverse(func(_, i interface{}) bool {
		item := i.(*poolClientItem) //nolint:forcetypeassert //...

		cerr = worker.NewJob(func(context.Context, uint64) error {
			cerr = item.client.Close()

			return nil
		})

		return true
	})

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	if cerr != nil {
		return e(cerr, "")
	}

	return nil
}

func (p *PoolClient) Add(addr *net.UDPAddr, client *Client) bool {
	return !p.clients.SetValue(addr.String(), client)
}

func (p *PoolClient) Remove(addr *net.UDPAddr) bool {
	found := p.clients.Exists(addr.String())

	if found {
		p.clients.RemoveValue(addr.String())
	}

	return found
}

func (p *PoolClient) Client(addr *net.UDPAddr) (*Client, bool) {
	switch i, found := p.clients.Value(addr.String()); {
	case !found:
		return nil, false
	case i == nil, util.IsNilLockedValue(i):
		return nil, false
	default:
		return i.(*Client), true //nolint:forcetypeassert //...
	}
}

func (p *PoolClient) Dial(
	ctx context.Context,
	addr *net.UDPAddr,
	newClient func(*net.UDPAddr) *Client,
) (quic.EarlyConnection, error) {
	var found bool
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item := i.(*poolClientItem) //nolint:forcetypeassert // ...
			item.accessed = time.Now()

			client = item.client

			found = true

			return nil, errors.Errorf("ignore")
		}

		client = newClient(addr)

		return &poolClientItem{
			client:   client,
			accessed: time.Now(),
		}, nil
	})

	if client == nil {
		return nil, net.ErrClosed
	}

	if found {
		session := client.Session()
		if session == nil {
			return nil, net.ErrClosed
		}

		return session, nil
	}

	session, err := client.Dial(ctx)
	if err != nil {
		go p.onerror(addr, client, err)

		return nil, err
	}

	return session, nil
}

func (p *PoolClient) Write(
	ctx context.Context,
	addr *net.UDPAddr,
	f func(io.Writer) error,
	newClient func(*net.UDPAddr) *Client,
) (quic.Stream, error) {
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item := i.(*poolClientItem) //nolint:forcetypeassert // ...
			item.accessed = time.Now()

			client = item.client

			return nil, errors.Errorf("ignore")
		}

		client = newClient(addr)

		return &poolClientItem{
			client:   client,
			accessed: time.Now(),
		}, nil
	})

	if client == nil {
		return nil, net.ErrClosed
	}

	r, err := client.Write(ctx, f)
	if err != nil {
		go p.onerror(addr, client, err)

		return nil, err
	}

	return r, nil
}

func (p *PoolClient) Clean(cleanDuration time.Duration) int {
	removeds := make([]string, p.clients.Len())

	var n int
	p.clients.Traverse(func(k interface{}, v interface{}) bool {
		item := v.(*poolClientItem) //nolint:forcetypeassert // ...
		if time.Since(item.accessed) > cleanDuration {
			removeds[n] = k.(string) //nolint:forcetypeassert // ...
			n++
		}

		return true
	})

	if n < 1 {
		return 0
	}

	for i := range removeds[:n] {
		_ = p.clients.Remove(removeds[i], nil)
	}

	return n
}

func (p *PoolClient) onerror(addr *net.UDPAddr, c *Client, err error) {
	if !IsNetworkError(err) {
		return
	}

	var client *Client

	switch i, found := p.clients.Value(addr.String()); {
	case !found:
		return
	case i == nil:
		return
	default:
		client = i.(*poolClientItem).client //nolint:forcetypeassert // ...
	}

	if client.id != c.id {
		return
	}

	_ = p.clients.Remove(addr.String(), nil)

	if p.onerrorf != nil {
		p.onerrorf(addr, c, err)
	}
}

type poolClientItem struct {
	client   *Client
	accessed time.Time
}
