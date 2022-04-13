package quicstream

import (
	"context"
	"net"
	"time"

	"github.com/lucas-clemente/quic-go"
	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type PoolClient struct {
	clients  *util.LockedMap
	onerrorf func(addr *net.UDPAddr, c *Client, err error)
}

func NewPoolClient() *PoolClient {
	return &PoolClient{
		clients: util.NewLockedMap(),
	}
}

func (p *PoolClient) Dial(
	ctx context.Context,
	addr *net.UDPAddr,
	newClient func(*net.UDPAddr) *Client,
) (quic.EarlySession, error) {
	var found bool
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item := i.(*poolClientItem)
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

	if found {
		return client.Session(), nil
	}

	session, err := client.Dial(ctx)
	if err != nil {
		go p.onerror(addr, client, err)

		return nil, errors.Wrap(err, "")
	}

	return session, nil
}

func (p *PoolClient) Send(
	ctx context.Context,
	addr *net.UDPAddr,
	b []byte,
	newClient func(*net.UDPAddr) *Client,
) (quic.Stream, error) {
	var client *Client
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item := i.(*poolClientItem)
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

	r, err := client.Send(ctx, b)
	if err != nil {
		go p.onerror(addr, client, err)

		return nil, errors.Wrap(err, "")
	}

	return r, nil
}

func (p *PoolClient) onerror(addr *net.UDPAddr, c *Client, err error) {
	if !isNetworkError(err) {
		return
	}

	var client *Client
	switch i, found := p.clients.Value(addr.String()); {
	case !found:
		return
	default:
		client = i.(*poolClientItem).client
	}

	if client.id != c.id {
		return
	}

	_ = p.clients.Remove(addr.String(), nil)

	if p.onerrorf != nil {
		p.onerrorf(addr, c, err)
	}
}

func (p *PoolClient) Clean(cleanDuration time.Duration) int {
	removeds := make([]string, p.clients.Len())

	var n int
	p.clients.Traverse(func(k interface{}, v interface{}) bool {
		item := v.(*poolClientItem)
		if time.Since(item.accessed) > cleanDuration {
			removeds[n] = k.(string)
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

type poolClientItem struct {
	client   *Client
	accessed time.Time
}