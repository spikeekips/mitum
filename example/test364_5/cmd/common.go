package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
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
	"github.com/spikeekips/mitum/example/test364_5/quicstream"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
	"github.com/spikeekips/mitum/util/valuehash"
)

type baseCommand struct {
	Bind  string `default:"${bind}" help:"by default, ${bind}"`
	Redis string `default:"127.0.0.1:6379"`

	ctx                context.Context
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
	workerOptions      *dop5.WorkerOptions
	openSubStreams     quicstream.NewOpenSubStreamsFunc
	rdb                *redis.Client
	enc                *jsonenc.Encoder
	workerAddrs        dutil.LockedMap[string, *net.UDPAddr]
	newAcceptSubStream quicstream.NewAcceptSubStreamsFunc
}

func (cmd *baseCommand) prepare(role string, log *zerolog.Logger) error {
	cmd.log = log

	cmd.ctx, cmd.ctxCancel = dutil.ContextWithCancelCause(context.Background())

	if err := cmd.prepareQuicStream(role); err != nil {
		return err
	}

	cmd.enc = jsonenc.NewEncoder(1 << 9)
	if err := cmd.addHinters(); err != nil {
		return err
	}

	cmd.rdb = redis.NewClient(&redis.Options{
		Addr: cmd.Redis,
	})
	if err := cmd.rdb.FlushAll(cmd.ctx).Err(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.ctx, time.Millisecond*333)
	defer cancel()

	if _, err := cmd.rdb.Ping(ctx).Result(); err != nil {
		return errors.WithMessage(err, "redis")
	}

	cmd.workerAddrs = dutil.NewSingleLockedMap[string, *net.UDPAddr]()

	return nil
}

func (cmd *baseCommand) addHinters() error {
	mustAddDecode(cmd.enc, base.DummyPublickeyHint, base.DummyPublickey{}, dop5.DecodeJSONDummyPublickey)
	mustAddDecode(cmd.enc, dop5.AccountOpFactHint, &dop5.AccountOpFact{}, nil)

	return nil
}

func (cmd *baseCommand) closef() {
	for _, f := range slices.Backward(cmd.closefs) {
		_ = f()
	}

	cmd.ctxCancel(nil)
}

func (cmd *baseCommand) prepareQuicStream(role string) error {
	var pconn net.PacketConn

	switch bind, err := net.ResolveUDPAddr("udp", cmd.Bind); {
	case err != nil:
		return err
	default:
		switch c, err := net.ListenUDP("udp", bind); {
		case err != nil:
			return err
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

	cmd.quicConfig = quicstream.DefaultQuicConfig()

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

	addHandler, removeHandler, handler, handlerClosef := quicstream.NewFuncPrefixStreamHandler()
	cmd.mustAddHandler = func(prefix string, h quicstream.StreamHandler) {
		if !addHandler([]byte(prefix), h) {
			panic(fmt.Sprintf("failed to add handler, %q", prefix))
		}
	}

	cmd.removeHandler = removeHandler

	listench := make(chan error)
	go func() {
		listench <- listen(handler)
	}()

	cmd.closefs = append(cmd.closefs, func() error {
		closef()
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
	pool, err := dutil.NewLockedMap[string, quic.Connection](nil, 1<<9, 1<<9)
	if err != nil {
		return err
	}

	dial, closef, err := quicstream.NewDialPool(pool, nil, time.Minute)
	if err != nil {
		return err
	}

	cmd.dialf = dial
	cmd.closefs = append(cmd.closefs, closef)

	openConnSubStreams, openConnSubStreamsClose, _ := quicstream.NewConnsAcceptSubStreams(cmd.ctx, time.Minute)
	cmd.openSubStreams = quicstream.NewFuncOpenSubStreams(openConnSubStreams)

	cmd.closefs = append(cmd.closefs, func() error { openConnSubStreamsClose(); return nil })

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

	cmd.request = func(prefix string) quicstream.RequestFunc {
		if _, found := handlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			return request(ctx, addr, tlsConfig, quicConfig, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(prefix)); err != nil {
					return ctx, err
				}

				return h(ctx, stream)
			})
		}
	}

	cmd.requestWork = func(workID, prefix string) quicstream.RequestFunc {
		if workID == "" {
			panic("empty work id")
		}

		if _, found := workHandlerPrefixesString[prefix]; !found {
			panic(fmt.Sprintf("unknown prefix, %q", prefix))
		}

		return func(ctx context.Context, addr net.Addr, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			return request(ctx, addr, tlsConfig, quicConfig, func(ctx context.Context, stream quic.Stream) (context.Context, error) {
				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(workID)); err != nil {
					return ctx, err
				}

				if err := quicstream.WriteStreamHandlerPrefix(stream, []byte(prefix)); err != nil {
					return ctx, err
				}

				return h(ctx, stream)
			})
		}
	}

	cmd.requestWorker = func(prefix string) requestWorkerFunc {
		request := cmd.request(prefix)

		return func(ctx context.Context, worker string, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return ctx, err
			}

			return request(ctx, addr, tlsConfig, quicConfig, h)
		}
	}

	cmd.requestWorkWorker = func(workID, prefix string) requestWorkerFunc {
		request := cmd.requestWork(workID, prefix)

		return func(ctx context.Context, worker string, tlsConfig *tls.Config, quicConfig *quic.Config, h quicstream.StreamHandler) (context.Context, error) {
			addr, err := cmd.workerAddr(worker)
			if err != nil {
				return ctx, err
			}

			return request(ctx, addr, tlsConfig, quicConfig, h)
		}
	}

	cmd.newAcceptSubStream = quicstream.NewFuncAcceptSubStreams(1 << 9)

	return nil
}

func (cmd *baseCommand) workerAddr(worker string) (*net.UDPAddr, error) {
	addr, found := cmd.workerAddrs.Value(worker)
	if !found {
		return nil, util.ErrNotFound.Errorf("unknown worker, %q", worker)
	}

	return addr, nil
}

func newTLSConfig(serverName string) (*testcerts.KeyPair, *tls.Config, error) {
	ca := testcerts.NewCA()

	kp, err := ca.NewKeyPair(serverName)
	if err != nil {
		return nil, nil, err
	}

	tlsConfig, err := kp.ConfigureTLSConfig(ca.GenerateTLSConfig())
	if err != nil {
		return nil, nil, err
	}

	tlsConfig.InsecureSkipVerify = true

	return kp, tlsConfig, nil
}

func readUint64(r io.Reader, i *uint64) (int, error) {
	switch p, n, err := frame.ReadSized(r, 8); {
	case err != nil && !errors.Is(err, io.EOF),
		n < 1 && errors.Is(err, io.EOF):
		return n, err
	default:
		_, err := binary.Decode(p, binary.BigEndian, i)

		return n, errors.WithStack(err)
	}
}

func writeUint64(w io.Writer, i uint64) error {
	return errors.WithStack(binary.Write(w, binary.BigEndian, i))
}

func readUint64s(r io.Reader, u ...*uint64) error {
	for i := range slices.Values(u) {
		if _, err := readUint64(r, i); err != nil {
			return err
		}
	}

	return nil
}

func writeUint64s(w io.Writer, u ...uint64) error {
	for i := range slices.Values(u) {
		if err := writeUint64(w, i); err != nil {
			return err
		}
	}

	return nil
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
	return frame.WriteFixedSlice(w, uint32(len(i)), func(yield func([]byte) bool) {
		slices.Values(i)(func(s string) bool {
			return yield([]byte(s))
		})
	})
}

func readFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.ReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return valuehash.NewBytes(b), nil
	})
}

func ensureReadFixedFrameHash(r io.Reader, i *util.Hash) (int, error) {
	return frame.EnsureReadFixedValue(r, i, func(b []byte) (util.Hash, error) {
		return valuehash.NewBytes(b), nil
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

func newFuncGetStateOwnersMurmur3(owners []string, share uint64) func(string) []string {
	o := uint64(len(owners))

	return func(stateKey string) []string {
		p := dutil.Murmur3(0, stateKey)

		owners := make([]string, 0, share)
		for i := range share {
			owners = append(owners, owners[(p+i)%o])
		}

		return owners
	}
}

func mustAddDecode[T any](enc *jsonenc.Encoder, ht hint.Hint, sample T, decode any) {
	err := jsonenc.AddDecode(enc, ht, sample, decode)
	if err != nil {
		panic(err)
	}
}
