package launch

import (
	"os"
	"sort"
	"testing"
	"time"

	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/stretchr/testify/suite"
)

type testEventLogging struct {
	suite.Suite
	st *leveldbstorage.Storage
}

func (t *testEventLogging) SetupTest() {
	t.st = leveldbstorage.NewMemStorage()
}

func (t *testEventLogging) TearDownTest() {
	t.st.Close()
}

func (t *testEventLogging) iterValues(el *EventLogging, name EventLoggerName) (rs []map[string]interface{}) {
	t.NoError(el.IterName(name, [2]int64{}, func(_ time.Time, _ int64, value []byte) (bool, error) {
		var m map[string]interface{}
		t.NoError(util.UnmarshalJSON(value, &m))

		rs = append(rs, m)

		return true, nil
	}, true, 3333333333))

	return rs
}

func (t *testEventLogging) TestNew() {
	t.Run("ok", func() {
		el := newEventLoggingWithStorage(t.st, os.Stdout)
		l, err := el.Register("showme")
		t.NoError(err)

		now := time.Now().UnixNano()
		l.Debug().Str("findme", "eatme").Msg("killme")

		var rs [][]byte

		var raddedAt time.Time
		var roffset int64

		t.NoError(el.IterName("showme", [2]int64{}, func(addedAt time.Time, offset int64, value []byte) (bool, error) {
			raddedAt = addedAt
			roffset = offset
			rs = append(rs, value)

			return true, nil
		}, true, 100))

		t.Equal(1, len(rs))
		t.True(roffset > now-int64(time.Second) || roffset < now+int64(time.Second))

		t.T().Log("added_at:", raddedAt.UnixNano())
		t.T().Log("offset:", roffset)
		t.T().Log("record:", string(rs[0]))

		var r map[string]interface{}
		t.NoError(util.UnmarshalJSON(rs[0], &r))

		t.T().Log("unmarshaled:", r)
		t.Equal("debug", r["level"])
		t.Equal("eatme", r["findme"])
		t.NotEmpty(r["time"])
		t.NotEmpty(r["caller"])
		t.Equal("killme", r["message"])
	})
}

func (t *testEventLogging) TestNames() {
	el := newEventLoggingWithStorage(t.st, os.Stdout)

	t.Run("ok", func() {
		name := EventLoggerName("showme")
		_, err := el.Register(name)
		t.NoError(err)

		l, found := el.Logger(name)
		t.True(found)

		l.Info().Str("a", "0").Msg("msg")

		rs := t.iterValues(el, name)
		t.T().Log("logs:", rs)

		t.Equal(1, len(rs))
		t.Equal("info", rs[0]["level"])
		t.Equal("msg", rs[0]["message"])
		t.Equal("0", rs[0]["a"])
	})

	t.Run("unknown logger", func() {
		name := EventLoggerName("findme")

		l, found := el.Logger(name)
		t.False(found)

		l.Error().Str("b", "1").Msg("msg")

		rs := t.iterValues(el, name)
		t.T().Log("logs:", rs)

		t.Equal(0, len(rs))

		rs = t.iterValues(el, UnknownEventLogger)
		t.T().Log("logs:", rs)

		t.Equal(1, len(rs))

		t.Equal("error", rs[0]["level"])
		t.Equal("msg", rs[0]["message"])
		t.Equal("1", rs[0]["b"])
	})
}

func (t *testEventLogging) TestIter() {
	el := newEventLoggingWithStorage(t.st, nil)

	names := []EventLoggerName{
		EventLoggerName(util.UUID().String()),
		EventLoggerName(util.UUID().String()),
		EventLoggerName(util.UUID().String()),
	}

	for i := range names {
		_, err := el.Register(names[i])
		t.NoError(err)
	}

	msgs := map[EventLoggerName][]string{}

	for i := range names {
		l, found := el.Logger(names[i])
		t.True(found)
		t.True(found)

		msgs[names[i]] = make([]string, (i+1)*3)

		for k := range msgs[names[i]] {
			l.Info().Int("a", k).Str("name", string(names[i])).Msg("msg")
		}
	}

	for i := range names {
		name := names[i]

		var rs []int

		el.IterName(name, [2]int64{}, func(_ time.Time, _ int64, value []byte) (bool, error) {
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(value, &m))

			t.Equal(name, EventLoggerName(m["name"].(string)))
			rs = append(rs, int(m["a"].(float64)))

			return true, nil
		}, true, 333333)

		r := msgs[name]

		t.Equal(len(r), len(msgs[name]))
	}
}

func (t *testEventLogging) TestIterNameLimit() {
	el := newEventLoggingWithStorage(t.st, nil)

	name := EventLoggerName(util.UUID().String())
	l, err := el.Register(name)
	t.NoError(err)

	rd := make([]int, 33)
	for i := range rd {
		rd[i] = i
		l.Debug().Int("i", i).Msg("msg")
	}

	t.Run("sort", func() {
		var limit uint64 = 3

		var rs []int
		t.NoError(el.IterName(name, [2]int64{}, func(_ time.Time, _ int64, value []byte) (bool, error) {
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(value, &m))

			rs = append(rs, int(m["i"].(float64)))

			return true, nil
		}, true, limit))

		t.Equal(rd[:limit], rs)
	})

	t.Run("reverse sort", func() {
		var limit uint64 = 3

		var rs []int
		t.NoError(el.IterName(name, [2]int64{}, func(_ time.Time, _ int64, value []byte) (bool, error) {
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(value, &m))

			rs = append(rs, int(m["i"].(float64)))

			return true, nil
		}, false, limit))

		rrd := make([]int, 33)
		copy(rrd, rd)

		sort.Sort(sort.Reverse(sort.IntSlice(rrd)))

		t.Equal(rrd[:limit], rs)
	})

	t.Run("offset", func() {
		var limit uint64 = 3

		var offset int64
		t.NoError(el.IterName(name, [2]int64{}, func(_ time.Time, o int64, value []byte) (bool, error) {
			offset = o

			return true, nil
		}, true, limit))

		var rs []int
		t.NoError(el.IterName(name, [2]int64{offset + 1}, func(_ time.Time, _ int64, value []byte) (bool, error) {
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(value, &m))

			rs = append(rs, int(m["i"].(float64)))

			return true, nil
		}, true, limit))

		t.Equal(rd[limit:limit*2], rs)
	})

	t.Run("limit offset", func() {
		var limit uint64 = 3

		var offset int64
		t.NoError(el.IterName(name, [2]int64{}, func(_ time.Time, o int64, value []byte) (bool, error) {
			offset = o

			return true, nil
		}, true, limit))

		var rs []int
		t.NoError(el.IterName(name, [2]int64{0, offset - 1}, func(_ time.Time, _ int64, value []byte) (bool, error) {
			var m map[string]interface{}
			t.NoError(util.UnmarshalJSON(value, &m))

			rs = append(rs, int(m["i"].(float64)))

			return true, nil
		}, true, limit))

		t.Equal(rd[:limit-1], rs)
	})
}

func TestEventLogging(t *testing.T) {
	suite.Run(t, new(testEventLogging))
}
