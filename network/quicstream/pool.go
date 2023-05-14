package quicstream

import (
	"context"
	"math"
	"net"
	"time"

	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/spikeekips/mitum/util"
)

type PoolClient struct {
	clients  *util.ShardedMap[string, *poolClientItem]
	onerrorf func(addr *net.UDPAddr, c *Client, err error)
}

func NewPoolClient() *PoolClient {
	clients, _ := util.NewShardedMap[string, *poolClientItem](math.MaxInt8)

	return &PoolClient{
		clients: clients,
	}
}

func (p *PoolClient) Close() error {
	e := util.StringError("close PoolClient")

	defer p.clients.Close()

	if p.clients.Len() < 1 {
		return nil
	}

	worker := util.NewErrgroupWorker(context.Background(), int64(p.clients.Len()))
	defer worker.Close()

	p.clients.Traverse(func(_ string, i *poolClientItem) bool {
		_ = worker.NewJob(func(context.Context, uint64) error {
			_ = i.client.Close()

			return nil
		})

		return true
	})

	worker.Done()

	if err := worker.Wait(); err != nil {
		return e.Wrap(err)
	}

	return nil
}

func (p *PoolClient) Add(addr *net.UDPAddr, client *Client) bool {
	return p.clients.SetValue(
		addr.String(),
		&poolClientItem{
			client:   client,
			accessed: time.Now(),
		},
	)
}

func (p *PoolClient) Remove(addr *net.UDPAddr) bool {
	return p.clients.RemoveValue(addr.String())
}

func (p *PoolClient) Client(addr *net.UDPAddr) (*Client, bool) {
	switch i, found := p.clients.Value(addr.String()); {
	case !found, i == nil:
		return nil, false
	default:
		return i.client, true
	}
}

func (p *PoolClient) Dial(
	ctx context.Context,
	addr *net.UDPAddr,
	newClient func(*net.UDPAddr) *Client,
) (quic.EarlyConnection, error) {
	var found bool
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i *poolClientItem, clientfound bool) (*poolClientItem, error) {
		if clientfound && i != nil {
			i.accessed = time.Now()

			client = i.client

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

func (p *PoolClient) OpenStream(
	ctx context.Context,
	addr *net.UDPAddr,
	newClient func(*net.UDPAddr) *Client,
) (reader quic.Stream, writer quic.Stream, _ error) {
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i *poolClientItem, found bool) (*poolClientItem, error) {
		if found && i != nil {
			i.accessed = time.Now()

			client = i.client

			return nil, errors.Errorf("ignore")
		}

		client = newClient(addr)

		return &poolClientItem{
			client:   client,
			accessed: time.Now(),
		}, nil
	})

	if client == nil {
		return nil, nil, net.ErrClosed
	}

	r, w, err := client.OpenStream(ctx)
	if err != nil {
		go p.onerror(addr, client, err)

		return nil, nil, err
	}

	return r, w, nil
}

func (p *PoolClient) Clean(cleanDuration time.Duration) int {
	removeds := make([]string, p.clients.Len())

	var n int
	p.clients.Traverse(func(k string, v *poolClientItem) bool {
		if time.Since(v.accessed) > cleanDuration {
			removeds[n] = k

			n++
		}

		return true
	})

	if n < 1 {
		return 0
	}

	for i := range removeds[:n] {
		_ = p.clients.RemoveValue(removeds[i])
	}

	return n
}

func (p *PoolClient) onerror(addr *net.UDPAddr, c *Client, err error) {
	if !IsNetworkError(err) {
		return
	}

	var client *Client

	switch i, found := p.clients.Value(addr.String()); {
	case !found, i == nil:
		return
	default:
		client = i.client
	}

	if client.id != c.id {
		return
	}

	_ = p.clients.RemoveValue(addr.String())

	if p.onerrorf != nil {
		p.onerrorf(addr, c, err)
	}
}

type poolClientItem struct {
	client   *Client
	accessed time.Time
}
