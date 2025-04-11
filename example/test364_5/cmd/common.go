package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"slices"
	"time"

	"github.com/madflojo/testcerts"
	"github.com/pkg/errors"
	"github.com/quic-go/quic-go"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	jsonenc "github.com/spikeekips/mitum/example/test364_5/encoder/json"
	"github.com/spikeekips/mitum/example/test364_5/frame"
	dhash "github.com/spikeekips/mitum/example/test364_5/hash"
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/valkey-io/valkey-go"
)

type pprofFlags struct {
	Out string `group:"pprof" help:"output directory" name:"out" placeholder:"directory"`
	CPU bool   `group:"pprof" name:"cpu"`
	Mem bool   `group:"pprof" name:"mem"`
}

type baseCommand struct { //betteralign:ignore
	Bind       string `default:"${bind}" help:"by default, ${bind}"`
	Redis      string `default:"redis://127.0.0.1:6379"`
	pprofFlags `embed:"" prefix:"pprof."`

	ctx                context.Context //nolint:containedctx
	ctxCancel          dutil.InErrorFunc[error]
	log                *zerolog.Logger
	closefs            []dutil.ErrorFunc
	tlsConfig          *tls.Config
	quicConfig         *quic.Config
	dialf              quicstream.DialCloseFunc
	request            func(prefix string) quicstream.RequestFunc
	requestWorker      func(prefix string) requestWorkerFunc
	requestWork        func(workID, prefix string) quicstream.RequestFunc
	requestWorkWorker  func(workID, prefix string) requestWorkerFunc
	mustAddHandler     func(string, quicstream.StreamHandler)
	removeHandler      func([]byte) bool
	openSubStreams     quicstream.NewOpenSubStreamsFunc
	db                 db
	enc                *jsonenc.Encoder
	workerAddrs        dutil.LockedMap[string, *net.UDPAddr]
	newAcceptSubStream quicstream.NewAcceptSubStreamsFunc
	logStreamHandler   func(quicstream.StreamHandler, ...string) quicstream.StreamHandler
}

func (cmd *baseCommand) prepare(role string, log *zerolog.Logger) error {
	cmd.log = log

	cmd.ctx, cmd.ctxCancel = dutil.ContextWithCancelCause(context.Background())

	if d := cmd.pprofFlags.Out; d != "" {
		switch fi, err := os.Stat(d); {
		case os.IsNotExist(err):
			if err := os.MkdirAll(d, 0o700); err != nil {
				return errors.WithStack(err)
			}
		case err != nil:
			return errors.WithStack(err)
		case !fi.IsDir():
			return errors.New("not directory")
		}
	}

	if err := cmd.prepareQuicStream(role); err != nil {
		return err
	}

	cmd.enc = jsonenc.NewEncoder(cmd.ctx, 1<<9) //nolint:mnd
	if err := cmd.addHinters(); err != nil {
		return err
	}

	if err := cmd.loadDB(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.ctx, time.Millisecond*333) //nolint:mnd
	defer cancel()

	if err := cmd.db.ping(ctx); err != nil {
		return err
	}

	cmd.workerAddrs = dutil.NewSingleLockedMap[string, *net.UDPAddr]()

	return nil
}

func addDecode[T any](enc *jsonenc.Encoder, ht hint.Hint, sample T, decode any) dutil.ErrorFunc {
	return func() error {
		return jsonenc.AddDecode(enc, ht, sample, decode)
	}
}

func (cmd *baseCommand) addHinters() error {
	return dutil.FuncChain(
		addDecode(cmd.enc, base.MPublickeyHint, &base.MPublickey{}, dop5.DecodeJSONMPublickey),
		addDecode(cmd.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, dop5.AccountOpFactDecodeJSON),
		addDecode(cmd.enc, dop5.AccountSMVHint, &dop5.AccountSMV{}, dop5.AccountSMVDecodeJSON),
		addDecode(cmd.enc, dop5.AccountSVHint, &dop5.AccountSV{}, dop5.AccountSVDecodeJSON),
		addDecode(cmd.enc, dop5.BaseStateHint, &dop5.BaseState{}, dop5.BaseStateDecodeJSON),
		addDecode(cmd.enc, manifestHint, &manifest{}, decodeManifest),
	)
}

func (cmd *baseCommand) closef() {
	for _, f := range slices.Backward(cmd.closefs) {
		_ = f()
	}

	_ = cmd.ctxCancel(nil)
}

func (cmd *baseCommand) prepareQuicStream(role string) error {
	var pconn net.PacketConn

	switch bind, err := net.ResolveUDPAddr("udp", cmd.Bind); {
	case err != nil:
		return errors.WithStack(err)
	default:
		switch c, err := net.ListenUDP("udp", bind); {
		case err != nil:
			return errors.WithStack(err)
		default:
			pconn = c
		}
	}

	var statelessResetKey *quic.StatelessResetKey

	switch kp, c, err := newTLSConfig(role); {
	case err != nil:
		return err
	default:
		cmd.tlsConfig = c
		statelessResetKey = quicstream.NewStatelessResetKey(kp.PrivateKey())
	}

	cmd.quicConfig = defaultQuicConfig()

	listen, closef, err := quicstream.Serve(
		cmd.ctx,
		pconn,
		cmd.tlsConfig,
		cmd.quicConfig,
		statelessResetKey,
	)
	if err != nil {
		return err
	}

	cmd.log.Debug().
		Str("bind", cmd.Bind).
		Bool("tls_insecure", cmd.tlsConfig.InsecureSkipVerify).
		Dict("quic_config", zerolog.Dict().
			Dur("HandshakeIdleTimeout", cmd.quicConfig.HandshakeIdleTimeout).
			Dur("MaxIdleTimeout", cmd.quicConfig.MaxIdleTimeout).
			Int64("MaxIncomingStreams", cmd.quicConfig.MaxIncomingStreams).
			Dur("KeepAlivePeriod", cmd.quicConfig.KeepAlivePeriod).
			Bool("Allow0RTT", cmd.quicConfig.Allow0RTT),
		).
		Msg("quicstream prepared")

	cmd.logStreamHandler = newFuncLogStreamHandler(cmd.log, zerolog.DebugLevel)

	addHandler, removeHandler, handler, handlerClosef := quicstream.NewFuncPrefixStreamHandler()
	cmd.mustAddHandler = func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), cmd.logStreamHandler(h, "accept", prefix)) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}

	cmd.removeHandler = removeHandler

	listench := make(chan error)
	go func() {
		listench <- listen(handler)
	}()

	cmd.closefs = append(cmd.closefs, func() error {
		_ = closef()

		handlerClosef()

		cmd.log.Debug().Err(<-listench).Msg("quicstream listen stopped")

		return nil
	})

	if err := cmd.prepareDialFunc(); err != nil {
		return err
	}

	return nil
}

func (cmd *baseCommand) prepareDialFunc() error {
	pool, err := dutil.NewLockedMap[string, quic.Connection](nil, 1<<9) //nolint:mnd
	if err != nil {
		return err
	}

	dial, closef, err := quicstream.NewDialPool(cmd.ctx, pool, nil, time.Minute)
	if err != nil {
		return err
	}

	cmd.dialf = dial
	cmd.closefs = append(cmd.closefs, closef)

	openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(cmd.ctx, time.Minute)
	cmd.openSubStreams = quicstream.NewFuncOpenSubStreams(openConnSubStreams)

	cmd.closefs = append(cmd.closefs, func() error {
		openConnSubStreamsClose()

		return nil
	})

	cmd.newAcceptSubStream = quicstream.NewFuncAcceptSubStreams(1 << 9) //nolint:mnd

	return cmd.prepareRequestFuncs()
}

func (cmd *baseCommand) prepareRequestFuncs() error {
	var request quicstream.RequestFunc

	switch r, err := quicstream.NewFuncRequest(
		quicstream.NewDialWithCloseFunc(cmd.dialf, quicstream.NewFuncConnClosingStreamError),
		cmd.tlsConfig, cmd.quicConfig,
	); {
	case err != nil:
		return err
	default:
		request = r
	}

	prefixRequest := func(p ...string) quicstream.RequestFunc {
		b := make([][]byte, len(p))
		for i, j := range slices.All(p) {
			b[i] = []byte(j)
		}

		r := quicstream.NewFuncPrefixRequest(request, b...)

		l := cmd.log.With().Strs("prefix", p).Logger()

		logp := make([]string, len(p)+1)
		logp[0] = "request"
		copy(logp[1:], p)

		return func(
			ctx context.Context,
			addr net.Addr,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
			h quicstream.StreamHandler,
		) (context.Context, error) {
			ctx, err := r(ctx, addr, tlsConfig, quicConfig, cmd.logStreamHandler(h, logp...))
			l.Trace().Err(err).Msg("request handler")

			return ctx, err
		}
	}

	cmd.request = func(prefix string) quicstream.RequestFunc {
		if _, found := handlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return prefixRequest(prefix)
	}

	cmd.requestWork = func(workID, prefix string) quicstream.RequestFunc {
		if workID == "" {
			panic("empty work id")
		}

		if _, found := workHandlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return prefixRequest(workID, prefix)
	}

	cmd.requestWorker = func(prefix string) requestWorkerFunc {
		r := prefixRequest(prefix)

		return func(
			ctx context.Context,
			worker string,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
			h quicstream.StreamHandler,
		) (context.Context, error) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return ctx, err
			}

			nctx, err := r(ctx, addr, tlsConfig, quicConfig, h)
			cmd.log.Trace().Err(err).Str("prefix", prefix).Str("worker", worker).Msg("request")

			return nctx, err
		}
	}

	cmd.requestWorkWorker = func(workID, prefix string) requestWorkerFunc {
		request := cmd.requestWork(workID, prefix)

		return func(
			ctx context.Context,
			worker string,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
			h quicstream.StreamHandler,
		) (context.Context, error) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return ctx, err
			}

			nctx, err := request(ctx, addr, tlsConfig, quicConfig, h)
			cmd.log.Trace().Err(err).Str("prefix", prefix).Str("work_id", workID).Str("worker", worker).Msg("request")

			return nctx, err
		}
	}

	return nil
}

func (cmd *baseCommand) workerAddr(worker string) (*net.UDPAddr, error) {
	addr, found := cmd.workerAddrs.Value(worker)
	if !found {
		return nil, util.ErrNotFound.Errorf("unknown worker, %q", worker)
	}

	return addr, nil
}

func (cmd *baseCommand) parseWorkersAddr(workers []string) error {
	for s := range slices.Values(workers) {
		switch addr, err := net.ResolveUDPAddr("udp", s); {
		case err != nil:
			return errors.WithStack(err)
		case addr.Port < 1:
			return errors.Errorf("invalid worker addr, %q", s)
		default:
			cmd.workerAddrs.SetValue(s, addr)
		}
	}

	return nil
}

func (cmd *baseCommand) newFuncMustAddHandler(
	addHandler func([]byte, quicstream.StreamHandler) bool,
) func(string, quicstream.StreamHandler) {
	return func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), cmd.logStreamHandler(h, prefix)) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}
}

func (cmd *baseCommand) pprof(name string) (func(), error) {
	var cpu, mem func()

	switch f, err := cmd.cpuPprof(name); {
	case err != nil:
		return nil, err
	default:
		cpu = f
	}

	switch f, err := cmd.memPprof(name); {
	case err != nil:
		return nil, err
	default:
		mem = f
	}

	return func() {
			cpu()
			mem()
		},
		nil
}

func (cmd *baseCommand) cpuPprof(name string) (func(), error) {
	if !cmd.pprofFlags.CPU {
		return func() {}, nil
	}

	return cpuPprof(filepath.Join(cmd.pprofFlags.Out, fmt.Sprintf("cpu-%s.pprof", name)))
}

func (cmd *baseCommand) memPprof(name string) (func(), error) {
	if !cmd.pprofFlags.Mem {
		return func() {}, nil
	}

	return memPprof(filepath.Join(cmd.pprofFlags.Out, fmt.Sprintf("mem-%s.pprof", name)))
}

func (cmd *baseCommand) loadDB() error {
	var dbtype, addr string

	switch u, err := url.Parse(cmd.Redis); {
	case err != nil:
		return errors.WithStack(err)
	default:
		dbtype = u.Scheme
		addr = u.Host
	}

	switch dbtype {
	case "redis":
		cmd.db = &redisDB{
			enc: cmd.enc,
			rdb: redis.NewClient(&redis.Options{
				Addr:       addr,
				MaxRetries: -1,
				PoolSize:   1 << 9, //nolint:mnd // NOTE enough pool size reduce the elapsed time to get values.
			}),
		}
	case "valkey":
		client, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:  []string{addr},
			DisableRetry: true,
		})
		if err != nil {
			return errors.WithStack(err)
		}

		cmd.db = &valkeyDB{enc: cmd.enc, client: client}
	default:
		return errors.Errorf("unknown db type, %q", dbtype)
	}

	return nil
}

func cpuPprof(out string) (func(), error) {
	f, err := os.Create(filepath.Clean(out))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if err := pprof.StartCPUProfile(f); err != nil {
		return nil, errors.WithStack(err)
	}

	return func() {
			pprof.StopCPUProfile()

			_ = f.Close()
		},
		nil
}

func memPprof(out string) (func(), error) {
	f, err := os.Create(filepath.Clean(out))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return func() {
			runtime.GC()
			_ = pprof.WriteHeapProfile(f)
			_ = f.Close()
		},
		nil
}

func newTLSConfig(serverName string) (*testcerts.KeyPair, *tls.Config, error) {
	ca := testcerts.NewCA()

	kp, err := ca.NewKeyPair(serverName)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	tlsConfig, err := kp.ConfigureTLSConfig(ca.GenerateTLSConfig())
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}

	tlsConfig.InsecureSkipVerify = true

	return kp, tlsConfig, nil
}

func readFixedFrameString(r io.Reader, i *string) (int, error) {
	return frame.ReadFixedValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func ensureReadFixedFrameString(r io.Reader, i *string) (int, error) {
	return frame.EnsureReadFixedValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func readFixedFrameStringSlice(r io.Reader, i *[]string) error {
	return frame.ReadFixedSliceValue(r, i, func(b []byte) (string, error) {
		return string(b), nil
	})
}

func writeFixedFrameStringSlice(w io.Writer, i []string) error {
	return frame.WriteFixedSlice(w, dutil.SafeIntToUint32(len(i)), func(yield func([]byte, error) bool) {
		for s := range slices.Values(i) {
			if !yield([]byte(s), nil) {
				return
			}
		}
	})
}

func readFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.ReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return dhash.BytesFromBytes(b), nil
	})
}

func ensureReadFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.EnsureReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return dhash.BytesFromBytes(b), nil
	})
}

func writeFixedFrameHashes(w io.Writer, u ...util.Hash) error {
	for i := range slices.Values(u) {
		if i == nil {
			continue
		}

		if err := frame.WriteFixed(w, i.Bytes()); err != nil {
			return err
		}
	}

	return nil
}

func newFuncGetStateOwnersMurmur3(workers []string, share uint64) func(string) []string {
	o := uint64(len(workers))

	return func(stateKey string) []string {
		p := dutil.Murmur3(0, stateKey)

		owners := make([]string, 0, share)
		for i := range share {
			owners = append(owners, workers[(p+i)%o])
		}

		return owners
	}
}

func writeOk(w io.Writer, ok bool) error {
	switch {
	case ok:
		return frame.WriteFixed(w, []byte{0x01})
	default:
		return frame.WriteFixed(w, nil)
	}
}

func readOk(r io.Reader) (bool, int, error) { //nolint:unparam
	switch b, n, err := frame.ReadFixed(r); {
	case err != nil && !errors.Is(err, io.EOF):
		return false, n, err
	case n < 1 && errors.Is(err, io.EOF):
		return false, n, frame.ErrInsufficientRead
	default:
		return len(b) > 0, n, nil
	}
}

func writeError(w io.Writer, err error) error {
	var s []byte
	if err != nil {
		s = []byte(err.Error())
	}

	return frame.WriteFixed(w, s)
}

func readError(r io.Reader, err *error) (int, error) {
	b, n, rerr := frame.ReadFixed(r)
	switch {
	case rerr != nil:
	case len(b) > 0:
		*err = errors.New(string(b))
	}

	return n, rerr
}

func defaultQuicConfig() *quic.Config {
	config := quicstream.DefaultQuicConfig()
	config.HandshakeIdleTimeout = time.Millisecond * 333 //nolint:mnd
	config.KeepAlivePeriod = time.Second

	return config
}

func newFuncLogStreamHandler(
	logger *zerolog.Logger,
	level zerolog.Level,
) func(quicstream.StreamHandler, ...string) quicstream.StreamHandler {
	return func(h quicstream.StreamHandler, prefix ...string) quicstream.StreamHandler {
		l := logger.With().Strs("prefix", prefix).Logger()

		fp := func() *zerolog.Event {
			return l.WithLevel(level) //nolint:zerologlint
		}

		return func(ctx context.Context, stream quic.Stream) (context.Context, error) {
			l := dutil.ElapsedLogError(fp)

			ctx, err := h(ctx, stream)

			l(err).Msg("handler")

			return ctx, err
		}
	}
}

func defaultNewFuncJobWorker(ctx context.Context, semSize int64) (
	dutil.JobWorkerNewJobFunc,
	func(),
	dutil.InErrorFunc[error],
	error,
) {
	return dutil.NewFuncJobWorker(ctx, min(semSize, 1<<9)) //nolint:mnd
}
