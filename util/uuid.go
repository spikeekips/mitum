package util

import (
	"crypto/rand"
	"io"
	"sync"
	"time"

	"github.com/oklog/ulid"
	uuid "github.com/satori/go.uuid"
)

func UUID() uuid.UUID {
	return uuid.NewV4()
}

type ULID struct {
	sync.Mutex
	entropy io.Reader
}

func NewULID() *ULID {
	return &ULID{
		entropy: ulid.Monotonic(rand.Reader, 0),
	}
}

func (u *ULID) New() ulid.ULID {
	u.Lock()
	defer u.Unlock()

	return ulid.MustNew(ulid.Timestamp(time.Now()), u.entropy)
}
