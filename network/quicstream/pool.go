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
	var item *poolClientItem
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item = i.(*poolClientItem)
			item.accessed = time.Now()

			found = true

			return nil, errors.Errorf("ignore")
		}

		item = &poolClientItem{
			client:   newClient(addr),
			accessed: time.Now(),
		}

		return item, nil
	})

	if found {
		return item.client.Session(), nil
	}

	session, err := item.client.Dial(ctx)
	if err != nil {
		go p.onerror(addr, item.client, err)

		return nil, errors.Wrap(err, "")
	}

	return session, nil
}

func (p *PoolClient) Send(
	ctx context.Context,
	addr *net.UDPAddr,
	b []byte,
	newClient func(*net.UDPAddr) *Client,
) ([]byte, error) {
	var item *poolClientItem
	_, _ = p.clients.Set(addr.String(), func(i interface{}) (interface{}, error) {
		if !util.IsNilLockedValue(i) {
			item = i.(*poolClientItem)
			item.accessed = time.Now()

			return nil, errors.Errorf("ignore")
		}

		item = &poolClientItem{
			client:   newClient(addr),
			accessed: time.Now(),
		}

		return item, nil
	})

	rb, err := item.client.Send(ctx, b)
	if err != nil {
		go p.onerror(addr, item.client, err)

		return rb, errors.Wrap(err, "")
	}

	return rb, nil
}

func (p *PoolClient) onerror(addr *net.UDPAddr, c *Client, err error) {
	if !isNetworkError(err) {
		return
	}

	var item *poolClientItem
	switch i, found := p.clients.Value(addr.String()); {
	case !found:
		return
	default:
		item = i.(*poolClientItem)
	}

	if item.client.id != c.id {
		return
	}

	_ = p.clients.Remove(addr.String(), nil)

	if p.onerrorf != nil {
		p.onerrorf(addr, c, err)
	}
}

func (p *PoolClient) Clean(cleanDuration time.Duration) int {
	var removeds []string
	p.clients.Traverse(func(k interface{}, v interface{}) bool {
		item := v.(*poolClientItem)
		if time.Since(item.accessed) > cleanDuration {
			removeds = append(removeds, k.(string))
		}

		return true
	})

	for i := range removeds {
		_ = p.clients.Remove(removeds[i], nil)
	}

	return len(removeds)
}

type poolClientItem struct {
	client   *Client
	accessed time.Time
}
