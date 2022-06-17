package util

import (
	"strings"
	"time"

	"github.com/pkg/errors"
)

// ParseRFC3339 parses RFC3339 string.
func ParseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errors.Wrap(err, "")
	}

	return t, nil
}

// RFC3339 formats time.Time to RFC3339Nano string.
func RFC3339(t time.Time) string {
	s := t.Format("2006-01-02T15:04:05.999999999")

	if len(s) < 29 { //nolint:gomnd //...
		s += strings.Repeat("0", 29-len(s))
	}

	return s + t.Format("Z07:00")
}

// NormalizeTime clear the nanoseconds part from Time and make time to UTC.
// "2009-11-10T23:00:00.00101010Z" -> "2009-11-10T23:00:00.001Z",
func NormalizeTime(t time.Time) time.Time {
	n := t.UTC()

	return time.Date(
		n.Year(),
		n.Month(),
		n.Day(),
		n.Hour(),
		n.Minute(),
		n.Second(),
		(n.Nanosecond()/1000000)*1000000, //nolint:gomnd //...
		time.UTC,
	)
}

func TimeString(t time.Time) string {
	return RFC3339(t)
}

func TimeEqual(a, b time.Time) bool {
	return NormalizeTime(a).Equal(NormalizeTime(b))
}
