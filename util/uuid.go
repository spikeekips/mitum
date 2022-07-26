package util

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid"
	uuid "github.com/satori/go.uuid"
)

var (
	ulidpool = NewULIDPool()
	ULIDLen  = len([]byte(ULID().String()))
)

func UUID() uuid.UUID {
	return uuid.NewV4()
}

type ULIDPool struct {
	entropy io.Reader
	sync.Mutex
}

func NewULIDPool() *ULIDPool {
	return &ULIDPool{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

func (u *ULIDPool) New() ulid.ULID {
	u.Lock()
	defer u.Unlock()

	return ulid.MustNew(ulid.Timestamp(time.Now()), u.entropy)
}

func ULID() ulid.ULID {
	return ulidpool.New()
}
