package launch

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/logging"
	"github.com/stretchr/testify/suite"
)

type testWatchUpdateFromConsulFunc struct {
	suite.Suite
	log *logging.Logging
}

func (t *testWatchUpdateFromConsulFunc) SetupSuite() {
	t.log = logging.TestLogging
}

func (t *testWatchUpdateFromConsulFunc) TestNew() {
	prefix := util.UUID().String()

	var currentValue string

	fs := map[string]updateDesignValueFunc{
		"showme": func(key, value string) (prev, next interface{}, _ error) {
			prev = currentValue
			currentValue = value

			t.T().Logf("key=%q value=%q prev=%q current=%q", key, value, prev, currentValue)

			return prev, currentValue, nil
		},
	}

	f := watchUpdateFromConsulFunc(prefix, fs, t.log)

	var createIndex uint64
	t.Run("short key", func() {
		updated, err := f("a", "", createIndex)
		t.False(updated)
		t.Error(err)
		t.ErrorContains(err, "wrong key")
	})

	t.Run("unknown key", func() {
		updated, err := f(prefix+"findme", "", createIndex)
		t.False(updated)
		t.Error(err)
		t.ErrorContains(err, "unknown key")
	})

	t.Run("ok", func() {
		value := util.UUID().String()

		updated, err := f(prefix+"showme", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value, currentValue)
	})

	t.Run("old", func() {
		updated, err := f(prefix+"showme", "", createIndex) // createIndex not updated
		t.False(updated)
		t.NoError(err)
	})

	t.Run("update again", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"showme", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value, currentValue)
	})
}

func (t *testWatchUpdateFromConsulFunc) TestSet() {
	prefix := util.UUID().String()

	var currentValue string

	fs := map[string]updateDesignValueFunc{
		"root": updateDesignSet(func(key, prefix, nextkey, value string) (prev interface{}, next interface{}, err error) {
			t.T().Logf("> root: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

			switch prefix {
			case "isaac":
				value += "-isaac"
			case "misc":
				value += "-misc"
			default:
				return prev, next, errors.Errorf("unknown key, %q", key)
			}

			prev = currentValue
			currentValue = value

			t.T().Logf("< root: prev=%q current=%q", prev, currentValue)

			return prev, currentValue, nil
		}),
		"child": updateDesignSet(func(key, prefix, nextkey, value string) (prev interface{}, next interface{}, err error) {
			t.T().Logf("> child: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

			switch prefix {
			case "isaac":
				value += "-child/isaac"
			case "misc":
				value += "-child/misc"

				return updateDesignSet(func(key, prefix, nextkey, value string) (prev interface{}, next interface{}, err error) {
					t.T().Logf("> child/misc: key=%q prefix=%q nextkey=%q value=%q", key, prefix, nextkey, value)

					switch prefix {
					case "a":
						value += "-a"
					case "b":
						value += "-b"
					default:
						return prev, next, errors.Errorf("unknown key, %q", key)
					}

					prev = currentValue
					currentValue = value

					t.T().Logf("< child/misc: prev=%q current=%q", prev, currentValue)

					return prev, currentValue, nil
				})(nextkey, value)
			default:
				return prev, next, errors.Errorf("unknown key, %q", key)
			}

			prev = currentValue
			currentValue = value

			t.T().Logf("< child: prev=%q current=%q", prev, currentValue)

			return prev, currentValue, nil
		}),
	}

	f := watchUpdateFromConsulFunc(prefix, fs, t.log)

	var createIndex uint64

	t.Run("ok", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"root/isaac", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-isaac", currentValue)
	})

	t.Run("end slash", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"root/misc/", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-misc", currentValue)
	})

	t.Run("child/misc/a", func() {
		createIndex++

		value := util.UUID().String()

		updated, err := f(prefix+"child/misc/a", value, createIndex)
		t.NoError(err)
		t.True(updated)

		t.Equal(value+"-child/misc-a", currentValue)
	})
}

func TestWatchUpdateFromConsulFunc(t *testing.T) {
	suite.Run(t, new(testWatchUpdateFromConsulFunc))
}
