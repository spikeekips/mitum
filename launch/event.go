package launch

import (
	"context"
	"io"
	"net"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacnetwork "github.com/spikeekips/mitum/isaac/network"
	"github.com/spikeekips/mitum/network/quicstream"
	quicstreamheader "github.com/spikeekips/mitum/network/quicstream/header"
	"github.com/spikeekips/mitum/storage"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/localtime"
	"github.com/spikeekips/mitum/util/ps"
	"github.com/spikeekips/mitum/util/valuehash"
	leveldbOpt "github.com/syndtr/goleveldb/leveldb/opt"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
	leveldbutil "github.com/syndtr/goleveldb/leveldb/util"
)

type EventLoggerName string

var (
	UnknownEventLogger EventLoggerName = "unknown"
	AllEventLogger     EventLoggerName = "all"
)

var (
	HandlerPrefixEventLoggingString = "event"
	HandlerPrefixEventLogging       = quicstream.HashPrefix(HandlerPrefixEventLoggingString)
	EventLoggingHeaderHint          = hint.MustNewHint("event-header-v0.0.1")
)

var AllEventLoggerNames = []EventLoggerName{
	AllEventLogger,
	UnknownEventLogger,
	NodeEventLoggerName,
	NodeReadWriteEventLogger,
	HandoverEventLogger,
}

var PNameEventLoggingNetworkHandlers = ps.Name("event-logging-network-handlers")

type EventLogging struct {
	db *eventDatabase
	w  io.Writer
	m  map[EventLoggerName]zerolog.Logger
	sync.RWMutex
}

func LoadDefaultEventStorage( //revive:disable-line:flag-parameter
	dir string, readonly bool,
) (*leveldbstorage.Storage, error) {
	switch fi, err := os.Stat(dir); {
	case os.IsNotExist(err):
		if readonly {
			return nil, err //nolint:wrapcheck //...
		}

		if err = os.MkdirAll(dir, 0o700); err != nil {
			return nil, errors.WithStack(err)
		}
	case err != nil:
		return nil, errors.WithStack(err)
	case !fi.IsDir():
		return nil, errors.Errorf("%q not directory", dir)
	}

	str, err := leveldbStorage.OpenFile(dir, readonly)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	st, err := leveldbstorage.NewStorage(
		str,
		&leveldbOpt.Options{ReadOnly: readonly},
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return st, nil
}

func NewEventLogging(root string, defaultwriter io.Writer) (*EventLogging, error) {
	switch st, err := LoadDefaultEventStorage(root, false); {
	case err != nil:
		return nil, err
	default:
		return newEventLoggingWithStorage(st, defaultwriter), nil
	}
}

func newEventLoggingWithStorage(st *leveldbstorage.Storage, defaultwriter io.Writer) *EventLogging {
	el := &EventLogging{
		db: newEventDatabase(st),
		w:  defaultwriter,
		m:  map[EventLoggerName]zerolog.Logger{},
	}

	_, _ = el.Register(UnknownEventLogger)

	return el
}

func (el *EventLogging) Close() error {
	return el.db.close()
}

func (el *EventLogging) Register(name EventLoggerName) (l zerolog.Logger, _ error) {
	if name == AllEventLogger {
		return l, errors.Errorf("%q, already registered", name)
	}

	switch _, found := el.m[name]; {
	case found:
		return l, errors.Errorf("%q, already registered", name)
	default:
		l := el.newLogger(name)
		el.m[name] = l

		return l, nil
	}
}

func (el *EventLogging) Logger(name EventLoggerName) (zerolog.Logger, bool) {
	switch i, found := el.m[name]; {
	case !found:
		return el.m[UnknownEventLogger], false
	default:
		return i, true
	}
}

func (el *EventLogging) Iter(
	offsets [2]int64, // [2]int64{start unix nano, end unix nano}
	callback func(addedAt time.Time, offset int64, raw []byte) (bool, error),
	sort bool,
	limit uint64,
) error {
	return el.db.iterAll(offsets, callback, sort, limit)
}

func (el *EventLogging) IterName(
	name EventLoggerName,
	offsets [2]int64, // [2]int64{start unix nano, end unix nano}
	callback func(addedAt time.Time, offset int64, raw []byte) (bool, error),
	sort bool,
	limit uint64,
) error {
	return el.db.iterName(name, offsets, callback, sort, limit)
}

func (el *EventLogging) newLogger(name EventLoggerName) zerolog.Logger {
	prefix := EventNameKeyPrefix(name)

	var w io.Writer = newEventWriter(prefix, el.db)
	if el.w != nil {
		w = zerolog.MultiLevelWriter(w, el.w)
	}

	return zerolog.New(w).With().
		Timestamp().
		Caller().
		Stack().
		Str("event", string(name)).
		Logger().
		Level(zerolog.DebugLevel)
}

var LeveldbLabelEventDatabase = leveldbstorage.KeyPrefix{0x02, 0x00}

type eventDatabase struct {
	pst          *leveldbstorage.PrefixStorage
	keyPrefixAll [32]byte
	sync.RWMutex
}

func newEventDatabase(st *leveldbstorage.Storage) *eventDatabase {
	pst := leveldbstorage.NewPrefixStorage(st, LeveldbLabelEventDatabase[:])

	return &eventDatabase{
		pst:          pst,
		keyPrefixAll: EventNameKeyPrefix(AllEventLogger),
	}
}

func (db *eventDatabase) close() error {
	db.Lock()
	defer db.Unlock()

	if db.pst == nil {
		return nil
	}

	if err := db.pst.Close(); err != nil {
		return err
	}

	db.pst = nil

	return nil
}

func (db *eventDatabase) put(prefix [32]byte, b []byte) error {
	var st *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return err
	default:
		st = i
	}

	batch := st.NewBatch()
	defer batch.Reset()

	now := localtime.Now()

	nk := util.ConcatBytesSlice(
		prefix[:],
		util.Int64ToBytes(now.UnixNano()),
	)

	fw, buf := util.NewBufferBytesFrameWriter()
	defer buf.Reset()

	_ = fw.Header()
	_, _ = fw.Writer().Write(b) // NOTE encoder is json(zerolog)

	batch.Put(nk, buf.Bytes())

	ak := util.ConcatBytesSlice(
		db.keyPrefixAll[:],
		util.Int64ToBytes(now.UnixNano()),
	)

	batch.Put(ak, nk)

	return st.Batch(batch, nil)
}

func (db *eventDatabase) iterAll(
	offsets [2]int64, // [2]int64{start unix nano, end unix nano}
	callback func(addedAt time.Time, offset int64, raw []byte) (bool, error),
	sort bool,
	limit uint64,
) error {
	var st *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return err
	default:
		st = i
	}

	return db.iter(db.keyPrefixAll, offsets,
		func(_, raw []byte) (bool, error) {
			t, offset, err := LoadEventInfoFromKey(raw)
			if err != nil {
				return false, err
			}

			switch b, found, err := st.Get(raw); {
			case err != nil:
				return false, err
			case !found:
				return false, nil
			default:
				i, err := LoadRawEvent(b)
				if err != nil {
					return false, err
				}

				return callback(t, offset, i)
			}
		}, sort, limit)
}

func (db *eventDatabase) iterName(
	name EventLoggerName,
	offsets [2]int64, // [2]int64{start unix nano, end unix nano}
	callback func(addedAt time.Time, offset int64, raw []byte) (bool, error),
	sort bool,
	limit uint64,
) error {
	return db.iter(EventNameKeyPrefix(name), offsets,
		func(key, raw []byte) (bool, error) {
			t, offset, err := LoadEventInfoFromKey(key)
			if err != nil {
				return false, err
			}

			switch b, err := LoadRawEvent(raw); {
			case err != nil:
				return false, err
			default:
				return callback(t, offset, b)
			}
		}, sort, limit)
}

func (db *eventDatabase) iter(
	prefix [32]byte,
	offsets [2]int64, // [2]int64{start unix nano, end unix nano}
	callback func([]byte, []byte) (bool, error),
	sort bool,
	limit uint64,
) error {
	var st *leveldbstorage.PrefixStorage

	switch i, err := db.st(); {
	case err != nil:
		return err
	default:
		st = i
	}

	r := leveldbutil.BytesPrefix(prefix[:])

	if offsets[0] > 0 {
		r.Start = util.ConcatBytesSlice(prefix[:], util.Int64ToBytes(offsets[0]))
	}

	if offsets[1] > 0 {
		r.Limit = util.ConcatBytesSlice(prefix[:], util.Int64ToBytes(offsets[1]))
	}

	var count uint64

	return st.Iter(r, func(key []byte, raw []byte) (bool, error) {
		if keep, err := callback(key, raw); err != nil || !keep {
			return false, err
		}

		count++

		return count < limit, nil
	}, sort)
}

func (db *eventDatabase) st() (*leveldbstorage.PrefixStorage, error) {
	db.RLock()
	defer db.RUnlock()

	if db.pst == nil {
		return nil, storage.ErrClosed.WithStack()
	}

	return db.pst, nil
}

type eventWriter struct {
	db     *eventDatabase
	prefix [32]byte
}

func newEventWriter(prefix [32]byte, db *eventDatabase) *eventWriter {
	return &eventWriter{prefix: prefix, db: db}
}

func (w *eventWriter) Write(b []byte) (int, error) {
	if err := w.db.put(w.prefix, b); err != nil {
		return 0, err
	}

	return len(b), nil
}

func EventNameKeyPrefix(name EventLoggerName) [32]byte {
	return [32]byte(valuehash.NewSHA256([]byte(name)).Bytes())
}

func LoadRawEvent(raw []byte) ([]byte, error) {
	var fr *util.BytesFrameReader

	switch i, buf, err := util.NewBufferBytesFrameReader(raw); {
	case err != nil:
		return nil, err
	default:
		defer buf.Reset()

		fr = i
	}

	if _, err := fr.Header(); err != nil {
		return nil, err
	}

	b, err := io.ReadAll(fr.Reader())

	return b, errors.WithStack(err)
}

func LoadEventInfoFromKey(k []byte) (t time.Time, offset int64, _ error) {
	switch {
	case len(k) < 40: //nolint:gomnd // prefix(32)+time(8)
		return t, 0, errors.Errorf("wrong size key")
	default:
		i, err := util.BytesToInt64(k[32:40])
		if err != nil {
			return t, 0, err
		}

		return time.Unix(0, i), i, nil
	}
}

type EventLoggingHeader struct {
	name EventLoggerName
	isaacnetwork.BaseHeader
	offsets [2]int64
	sort    bool
	limit   uint64
}

func NewEventLoggingHeader(name EventLoggerName, offsets [2]int64, limit uint64, sort bool) EventLoggingHeader {
	return EventLoggingHeader{
		BaseHeader: isaacnetwork.BaseHeader{
			BaseRequestHeader: quicstreamheader.NewBaseRequestHeader(EventLoggingHeaderHint, HandlerPrefixEventLogging),
		},
		name:    name,
		offsets: offsets,
		limit:   limit,
		sort:    sort,
	}
}

func (h EventLoggingHeader) IsValid([]byte) error {
	e := util.StringError("EventLoggingHeader")

	if err := h.BaseHeader.IsValid(nil); err != nil {
		return e.Wrap(err)
	}

	switch {
	case h.offsets[0] < 0:
		return e.Errorf("invalid start offset")
	case h.offsets[1] < 0:
		return e.Errorf("invalid limit offset")
	case h.offsets[0] > 0 && h.offsets[1] > 0 && h.offsets[0] <= h.offsets[1]:
		return e.Errorf("invalid offsets")
	}

	if h.limit < 1 {
		return e.Errorf("invalid limit")
	}

	return nil
}

func (h EventLoggingHeader) Name() EventLoggerName {
	return h.name
}

func (h EventLoggingHeader) Offsets() [2]int64 {
	return h.offsets
}

func (h EventLoggingHeader) Limit() uint64 {
	return h.limit
}

func (h EventLoggingHeader) Sort() bool {
	return h.sort
}

type eventLoggingHeaderJSONMarshaler struct {
	Name    EventLoggerName `json:"name"`
	Offsets [2]int64        `json:"offsets"`
	Sort    bool            `json:"sort"`
	Limit   uint64          `json:"limit"`
}

func (h EventLoggingHeader) MarshalJSON() ([]byte, error) {
	return util.MarshalJSON(struct {
		isaacnetwork.BaseHeaderJSONMarshaler
		eventLoggingHeaderJSONMarshaler
	}{
		BaseHeaderJSONMarshaler: h.BaseHeader.JSONMarshaler(),
		eventLoggingHeaderJSONMarshaler: eventLoggingHeaderJSONMarshaler{
			Name:    h.name,
			Offsets: h.offsets,
			Limit:   h.limit,
			Sort:    h.sort,
		},
	})
}

func (h *EventLoggingHeader) DecodeJSON(b []byte, _ *jsonenc.Encoder) error {
	if err := util.UnmarshalJSON(b, &h.BaseHeader); err != nil {
		return err
	}

	var u eventLoggingHeaderJSONMarshaler
	if err := util.UnmarshalJSON(b, &u); err != nil {
		return err
	}

	h.name = u.Name
	h.offsets = u.Offsets
	h.limit = u.Limit
	h.sort = u.Sort

	return nil
}

func PEventLoggingNetworkHandlers(pctx context.Context) (context.Context, error) {
	var local base.LocalNode
	var isaacparams *isaac.Params
	var eventLogging *EventLogging

	if err := util.LoadFromContextOK(pctx,
		LocalContextKey, &local,
		ISAACParamsContextKey, &isaacparams,
		EventLoggingContextKey, &eventLogging,
	); err != nil {
		return pctx, err
	}

	var gerror error

	EnsureHandlerAdd(pctx, &gerror,
		HandlerPrefixEventLoggingString,
		QuicstreamHandlerEventLogging(
			local.Publickey(),
			isaacparams.NetworkID(),
			eventLogging,
			333, //nolint:gomnd //...
		),
		nil,
	)

	return pctx, gerror
}

func QuicstreamHandlerEventLogging(
	local base.Publickey,
	networkID base.NetworkID,
	eventLogging *EventLogging,
	maxItem uint64,
) quicstreamheader.Handler[EventLoggingHeader] {
	return func(
		ctx context.Context, addr net.Addr, broker *quicstreamheader.HandlerBroker, header EventLoggingHeader,
	) (context.Context, error) {
		m := maxItem

		if header.Limit() < m {
			m = header.Limit()
		}

		if err := isaacnetwork.QuicstreamHandlerVerifyNode(
			ctx, addr, broker,
			local, networkID,
		); err != nil {
			return ctx, err
		}

		f := func(addedAt time.Time, offset int64, raw []byte) (bool, error) {
			fw, buf := util.NewBufferBytesFrameWriter()
			defer buf.Reset()

			_ = fw.Header(
				[]byte(util.TimeString(addedAt)),
				util.Int64ToBytes(offset),
			)

			_, _ = fw.Writer().Write(raw)

			return true, broker.WriteBody(
				ctx,
				quicstreamheader.FixedLengthBodyType,
				uint64(buf.Len()),
				buf,
			)
		}

		var eerr error

		switch header.Name() {
		case "", AllEventLogger:
			eerr = eventLogging.Iter(header.Offsets(), f, header.Sort(), m)
		default:
			eerr = eventLogging.IterName(header.Name(), header.Offsets(), f, header.Sort(), m)
		}

		return ctx, broker.WriteResponseHeadOK(ctx, eerr == nil, eerr)
	}
}

func EventLoggingFromNetworkHandler(
	ctx context.Context,
	stream quicstreamheader.StreamFunc,
	priv base.Privatekey,
	networkID base.NetworkID,
	logger EventLoggerName,
	offsets [2]int64,
	limit uint64,
	sort bool,
	f func(addedAt time.Time, offset int64, raw []byte) (bool, error),
) error {
	header := NewEventLoggingHeader(logger, offsets, limit, sort)
	if err := header.IsValid(nil); err != nil {
		return err
	}

	read := func(r io.Reader) error {
		fr, err := util.NewBytesFrameReader(r)
		if err != nil {
			return err
		}

		var addedAt time.Time
		var offset int64

		hs, err := fr.Header()

		switch {
		case err != nil:
			return err
		case len(hs) < 2:
			return errors.Errorf("empty header")
		}

		t, err := util.ParseRFC3339(string(hs[0]))
		if err != nil {
			return errors.WithMessage(err, "added_at")
		}

		addedAt = t

		i, err := util.BytesToInt64(hs[1])
		if err != nil {
			return errors.WithMessage(err, "offset")
		}

		offset = i

		b, err := fr.Body()
		if err != nil {
			return errors.WithMessage(err, "body")
		}

		if keep, err := f(addedAt, offset, b); err != nil || !keep {
			return err
		}

		return nil
	}

	return stream(ctx, func(ctx context.Context, broker *quicstreamheader.ClientBroker) error {
		if err := broker.WriteRequestHead(ctx, header); err != nil {
			return err
		}

		if err := isaacnetwork.VerifyNode(ctx, broker, priv, networkID); err != nil {
			return err
		}

		for {
			switch _, _, r, _, res, err := broker.ReadBody(ctx); {
			case err != nil:
				return err
			case res != nil:
				return res.Err()
			default:
				if err := read(r); err != nil {
					return err
				}
			}
		}
	})
}
