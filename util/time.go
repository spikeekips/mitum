package util

import (
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

// ParseRFC3339 parses RFC3339 string.
func ParseRFC3339(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, errors.WithStack(err)
	}

	return t, nil
}

// RFC3339 formats time.Time to RFC3339Nano string.
func RFC3339(t time.Time) string {
	s := t.Format("2006-01-02T15:04:05.999999999")

	if len(s) < 29 { //nolint:mnd //...
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
		(n.Nanosecond()/1_000_000)*1_000_000, //nolint:mnd //...
		time.UTC,
	)
}

func TimeString(t time.Time) string {
	return RFC3339(t)
}

func TimeEqual(a, b time.Time) bool {
	return NormalizeTime(a).Equal(NormalizeTime(b))
}

func ParseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, errors.Wrap(err, "parse duration")
	}

	return time.Duration(i), nil
}

type ReadableDuration time.Duration

func (d ReadableDuration) MarshalText() ([]byte, error) {
	return []byte(time.Duration(d).String()), nil
}

func (d *ReadableDuration) UnmarshalJSON(b []byte) error {
	var i interface{}
	if err := UnmarshalJSON(b, &i); err != nil {
		return err
	}

	switch t := i.(type) {
	case int64:
		*d = ReadableDuration(time.Duration(t))
	case string:
		j, err := time.ParseDuration(t)
		if err != nil {
			return errors.Wrap(err, "unmarshal ReadableJSONDuration")
		}

		*d = ReadableDuration(j)
	default:
		return errors.Errorf("unknown duration format, %q", string(b))
	}

	return nil
}

func (d *ReadableDuration) UnmarshalYAML(y *yaml.Node) error {
	var i interface{}
	if err := y.Decode(&i); err != nil {
		return errors.WithStack(err)
	}

	switch t := i.(type) {
	case int64:
		*d = ReadableDuration(time.Duration(t))
	case string:
		j, err := time.ParseDuration(t)
		if err != nil {
			return errors.Wrap(err, "unmarshal ReadableJSONDuration")
		}

		*d = ReadableDuration(j)
	default:
		return errors.Errorf("unknown duration format, %v", i)
	}

	return nil
}
