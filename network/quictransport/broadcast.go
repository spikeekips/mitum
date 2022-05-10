package quictransport

import (
	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog"
)

type Broadcast struct {
	notifych chan struct{}
	id       string
	b        []byte
}

func NewBroadcast(b []byte, id string, notifych chan struct{}) *Broadcast {
	// NOTE size of b should be checked; it should be smaller than
	// UDPBufferSize.
	return &Broadcast{b: b, id: id, notifych: notifych}
}

func (b *Broadcast) Invalidates(o memberlist.Broadcast) bool {
	i, ok := o.(*Broadcast)

	return ok && i.id == b.id
}

func (b *Broadcast) Message() []byte {
	return b.b
}

func (b *Broadcast) Finished() {
	if b.notifych != nil {
		go func() {
			b.notifych <- struct{}{}

			close(b.notifych)
		}()
	}
}

func (*Broadcast) UniqueBroadcast() {}

func (b *Broadcast) MarshalZerologObject(e *zerolog.Event) {
	e.Str("id", b.id)
}
