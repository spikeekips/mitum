package util

import (
	"fmt"
	"strings"
	"time"

	semver "github.com/Masterminds/semver/v3"
	"github.com/pkg/errors"
	stdsemver "golang.org/x/mod/semver"
)

var MinVersionLength = 2

type Version struct {
	s          string
	prerelease string
	major      uint64
	minor      uint64
	patch      uint64
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
		return Version{}, ErrInvalid.Errorf("invalid version string, %q", s)
	}

	v, err := semver.NewVersion(s)
	if err != nil {
		return Version{}, ErrInvalid.Wrapf(err, "version string=%q", s)
	}

	p := newVersion(v)
	if err := p.IsValid(nil); err != nil {
		return Version{}, ErrInvalid.Wrap(err)
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
	case len(s) < MinVersionLength:
		return ErrInvalid.Errorf("empty version string")
	case !strings.HasPrefix(s, "v"):
		return ErrInvalid.Errorf("invalid version string, %q", s)
	case !stdsemver.IsValid(v.s):
		return ErrInvalid.Errorf("invalid semver, %q", s)
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
		return errors.WithMessage(err, "failed to unmarshal version")
	}

	*v = u

	return nil
}

type BuildInfo struct {
	Branch    string
	Commit    string
	BuildTime time.Time
	Version   Version
}

func ParseBuildInfo(version, branch, commit, buildTime string) (BuildInfo, error) {
	bi := BuildInfo{}

	switch i, err := ParseVersion(version); {
	case err != nil:
		return bi, err
	default:
		bi.Version = i
	}

	if buildTime != "-" {
		switch i, err := ParseRFC3339(buildTime); {
		case err != nil:
			return bi, err
		default:
			bi.BuildTime = i
		}
	}

	bi.Branch = branch
	bi.Commit = commit

	return bi, nil
}

func (bi BuildInfo) String() string {
	return fmt.Sprintf(`* mitum build info
  version: %s
   branch: %s
   commit: %s
    build: %s`, bi.Version, bi.Branch, bi.Commit, bi.BuildTime)
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
		x, y = x[1:], y[1:] // skip - or .

		var dx, dy string
		dx, x = versionNextIdent(x)
		dy, y = versionNextIdent(y)

		if dx == dy {
			continue
		}

		ix, iy := versionIsNum(dx), versionIsNum(dy)

		switch {
		case ix != iy:
			if ix {
				return -1
			}

			return 1
		case ix:
			if len(dx) < len(dy) {
				return -1
			}

			if len(dx) > len(dy) {
				return 1
			}
		case dx < dy:
			return -1
		default:
			return 1
		}
	}

	if x == "" {
		return -1
	}

	return 1
}

func versionNextIdent(x string) (dx, rest string) {
	i := 0
	for i < len(x) && x[i] != '.' { // revive:disable-line:optimize-operands-order
		i++
	}

	return x[:i], x[i:]
}

func versionIsNum(v string) bool {
	i := 0
	for i < len(v) && '0' <= v[i] && v[i] <= '9' { // revive:disable-line:optimize-operands-order
		i++
	}

	return i == len(v)
}
