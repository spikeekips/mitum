package quicmemberlist

import (
	"sync"

	"github.com/hashicorp/memberlist"
	"github.com/rs/zerolog"
)

type Broadcast struct {
	notifych     chan struct{} // revive:disable-line:nested-structs
	id           string
	b            []byte
	finishedonce sync.Once
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
	b.finishedonce.Do(func() {
		if b.notifych == nil {
			return
		}

		close(b.notifych)
	})
}

func (*Broadcast) UniqueBroadcast() {}

func (b *Broadcast) MarshalZerologObject(e *zerolog.Event) {
	e.Str("id", b.id)
}
