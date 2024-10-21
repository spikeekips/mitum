package util

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/oklog/ulid/v2"
)

var (
	ulidpool = NewULIDPool()
	ULIDLen  = len([]byte(ULID().String()))
)

func UUID() uuid.UUID {
	for {
		if i, err := uuid.NewV4(); err == nil {
			return i
		}
	}
}

type ULIDPool struct {
	entropy io.Reader
	l       sync.Mutex
}

func NewULIDPool() *ULIDPool {
	return &ULIDPool{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

func (u *ULIDPool) New() ulid.ULID {
	u.l.Lock()
	defer u.l.Unlock()

	return ulid.MustNew(ulid.Timestamp(time.Now()), u.entropy)
}

func ULID() ulid.ULID {
	return ulidpool.New()
}
