package util

import (
	"strings"

	semver "github.com/Masterminds/semver/v3"
	"github.com/pkg/errors"
	stdsemver "golang.org/x/mod/semver"
)

type Version struct {
	s          string
	major      uint64
	minor      uint64
	patch      uint64
	prerelease string
}

// EnsureParseVersion tries to parse version string, but skips to check
// IsValid().
func EnsureParseVersion(s string) Version {
	if !strings.HasPrefix(s, "v") {
		return Version{}
	}

	v, err := semver.NewVersion(s)
	if err != nil {
		return Version{}
	}

	return newVersion(v)
}

// ParseVersion tries to parse version string and also checks IsValid().
func ParseVersion(s string) (Version, error) {
	if !strings.HasPrefix(s, "v") {
		return Version{}, InvalidError.Errorf("invalid version string, %q", s)
	}

	v, err := semver.NewVersion(s)
	if err != nil {
		return Version{}, InvalidError.Wrapf(err, "version string=%q", s)
	}

	p := newVersion(v)
	if err := p.IsValid(nil); err != nil {
		return Version{}, InvalidError.Wrap(err)
	}

	return p, nil
}

func MustNewVersion(s string) Version {
	v, err := ParseVersion(s)
	if err != nil {
		panic(err)
	}

	return v
}

func newVersion(v *semver.Version) Version {
	return Version{
		s:          "v" + v.String(),
		major:      v.Major(),
		minor:      v.Minor(),
		patch:      v.Patch(),
		prerelease: v.Prerelease(),
	}
}

func (v Version) String() string { return v.s }

func (v Version) IsValid([]byte) error {
	switch s := strings.TrimSpace(v.s); {
	case len(s) < 2:
		return InvalidError.Errorf("empty version string")
	case !strings.HasPrefix(s, "v"):
		return InvalidError.Errorf("invalid version string, %q", s)
	case !stdsemver.IsValid(v.s):
		return InvalidError.Errorf("invalid semver, %q", s)
	default:
		return nil
	}
}

func (v Version) Major() uint64 {
	return v.major
}

func (v Version) Minor() uint64 {
	return v.minor
}

func (v Version) Patch() uint64 {
	return v.patch
}

func (v Version) Prerelease() string {
	return v.prerelease
}

func (v Version) Compare(b Version) int {
	if i := compareVersionMainPart(v.major, b.major); i != 0 {
		return i
	}

	if i := compareVersionMainPart(v.minor, b.minor); i != 0 {
		return i
	}

	if i := compareVersionMainPart(v.patch, b.patch); i != 0 {
		return i
	}

	return compareVersionPrerelease(v.prerelease, b.prerelease)
}

// IsCompatible checks if the given version is compatible to the target. The
// compatible conditions are major matches.
func (v Version) IsCompatible(b Version) bool {
	return v.major == b.major
}

func (v Version) IsEmpty() bool {
	return len(v.s) < 1
}

func (v Version) MarshalText() ([]byte, error) {
	return []byte(v.s), nil
}

func (v *Version) UnmarshalText(b []byte) error {
	u, err := ParseVersion(string(b))
	if err != nil {
		return errors.Wrap(err, "failed to unmarshal version")
	}

	*v = u

	return nil
}

func compareVersionMainPart(a, b uint64) int {
	switch {
	case a == b:
		return 0
	case a < b:
		return -1
	default:
		return 1
	}
}

// comparePrerelease compare prerelease string of version; it comes from
// golang.org/x/mod@v0.5.1/semver/semver.go.
func compareVersionPrerelease(a, b string) int {
	switch {
	case a == b:
		return 0
	case a == "":
		return 1
	case b == "":
		return -1
	}

	x := a
	y := b
	for x != "" && y != "" {
		x = x[1:] // skip - or .
		y = y[1:] // skip - or .

		var dx, dy string
		dx, x = versionNextIdent(x)
		dy, y = versionNextIdent(y)
		if dx == dy {
			continue
		}

		ix := versionIsNum(dx)
		iy := versionIsNum(dy)
		if ix != iy {
			if ix {
				return -1
			}

			return 1
		}

		if ix {
			if len(dx) < len(dy) {
				return -1
			}

			if len(dx) > len(dy) {
				return 1
			}
		}

		if dx < dy {
			return -1
		}

		return 1
	}

	if x == "" {
		return -1
	}

	return 1
}

func versionNextIdent(x string) (dx, rest string) {
	i := 0
	for i < len(x) && x[i] != '.' {
		i++
	}
	return x[:i], x[i:]
}

func versionIsNum(v string) bool {
	i := 0
	for i < len(v) && '0' <= v[i] && v[i] <= '9' {
		i++
	}
	return i == len(v)
}
