package localtime

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
)

type Time struct {
	time.Time
}

func New(t time.Time) Time {
	return Time{Time: t}
}

func (t Time) Bytes() []byte {
	return []byte(t.Normalize().String())
}

func (t Time) UTC() Time {
	return New(t.Time.UTC())
}

func (t Time) RFC3339() string {
	return util.RFC3339(t.Time)
}

func (t Time) Normalize() Time {
	return Time{Time: util.NormalizeTime(t.Time)}
}

func (t Time) Equal(n Time) bool {
	return t.Time.Equal(n.Time)
}

func (t Time) MarshalText() ([]byte, error) {
	return []byte(t.Normalize().RFC3339()), nil
}

func (t *Time) UnmarshalText(b []byte) error {
	s, err := util.ParseRFC3339(string(b))
	if err != nil {
		return errors.WithMessage(err, "unmarshal Time")
	}

	t.Time = util.NormalizeTime(s)

	return nil
}
