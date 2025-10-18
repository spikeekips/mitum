package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/btcsuite/btcd/btcec/v2"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/spikeekips/mitum/base"
	dop5 "github.com/spikeekips/mitum/example/test364_5"
	"github.com/spikeekips/mitum/example/test364_5/encoder"
	dutil "github.com/spikeekips/mitum/example/test364_5/util"
	dop7 "github.com/spikeekips/mitum/example/test364_7"
	"github.com/spikeekips/mitum/example/test364_7/frame"
	"github.com/spikeekips/mitum/util"
)

type cliControl struct {
	baseCommand
	NOpShare         uint64      `arg:"" name:"op-share" help:"op share"`
	NStateValueShare uint64      `arg:"" name:"state-value-share" help:"state value share"`
	NOps             uint64      `arg:"" name:"num-ops" help:"number of operations"`
	Workers          uint64      `arg:"" help:"number of workers" default:"1"`
	Launch           string      `default:"run.sh"`
	Root             string      `default:"./"`
	StartHeight      base.Height `name:"start-height" default:"33"`
	Round            uint64      `name:"round" default:"3"`
	nOpShare         uint64
	nStateValueShare uint64
	nOps             uint64
	pubs             []base.Publickey
	ophs             [][2]util.Hash
	rootDir          string
	launchProg       string
}

func (cmd *cliControl) Run(log *zerolog.Logger) error {
	/* FIXME
	1. generate
	- keypairs
	- operations
	1. launch new manager and workers
	1. send proposal to manager
	1. wait result from manager and workers
	1. verify result
	*/

	if err := cmd.baseCommand.prepare(log); err != nil {
		return err
	}

	if err := cmd.validateFlags(); err != nil {
		return err
	}

	if err := cmd.generate(); err != nil {
		return err
	}

	if err := cmd.launch(); err != nil {
		return err
	}

	return nil
}

func (cmd *cliControl) validateFlags() error {
	cmd.log.Debug().
		Uint64("op-share", cmd.NOpShare).
		Uint64("state-value-share", cmd.NStateValueShare).
		Uint64("workers", cmd.Workers).
		Uint64("ops", cmd.NOps).
		Interface("--start-height", cmd.StartHeight).
		Uint64("--round", cmd.Round).
		Str("--root", cmd.Root).
		Str("--launch", cmd.Launch).
		Msg("flags")

	switch {
	case cmd.Workers < 1:
		return errors.New("zero <workers>")
	case cmd.NOps < 1:
		return errors.New("zero <num-ops>")
	case cmd.NOpShare < 1:
		return errors.New("zero <op-share>")
	case cmd.NStateValueShare < 1:
		return errors.New("zero <state-value-share>")
	}

	if cmd.NOpShare < cmd.Workers {
		cmd.nOpShare = cmd.Workers
	}

	if cmd.NStateValueShare < cmd.Workers {
		cmd.nStateValueShare = cmd.Workers
	}

	switch {
	case cmd.Root == "":
		cmd.rootDir = "./"
	default:
		if _, err := os.Stat(filepath.Clean(cmd.Root)); err != nil {
			return err
		}

		cmd.rootDir = cmd.Root
	}

	switch i, err := filepath.Abs(cmd.rootDir); {
	case err != nil:
		return err
	default:
		cmd.rootDir = i
	}

	switch {
	case cmd.Launch == "":
		return errors.New("empty --launch")
	default:
		if _, err := os.Stat(filepath.Clean(cmd.Launch)); err != nil {
			return err
		}

		switch i, err := filepath.Abs(cmd.Launch); {
		case err != nil:
			return err
		default:
			cmd.launchProg = i
		}
	}

	cmd.log.Debug().
		Uint64("op-share", cmd.nOpShare).
		Uint64("state-value-share", cmd.nStateValueShare).
		Str("--root", cmd.rootDir).
		Str("--launch", cmd.launchProg).
		Msg("validated")

	return nil
}

func (cmd *cliControl) generate() error {
	if err := cmd.generateKeypairs(); err != nil {
		return err
	}

	if err := cmd.generateOps(); err != nil {
		return err
	}

	return nil
}

func (cmd *cliControl) generateKeypairs() error {
	size := cmd.NOps + 1

	nCreated, pubs, err := generateControlItems(
		cmd.ctx,
		filepath.Join(cmd.rootDir, "pubs.b"),
		size,
		func(i uint64) (base.Publickey, []byte, error) {
			_, k := btcec.PrivKeyFromBytes([]byte(strconv.FormatUint(i, 10)))

			return base.NewMPublickey(k), k.SerializeCompressed(), nil
		},
		func(b []byte, _ uint64) (base.Publickey, error) {
			k, err := btcec.ParsePubKey(b)
			if err != nil {
				return nil, errors.WithStack(err)
			}

			return base.NewMPublickey(k), nil
		},
	)
	if err != nil {
		return err
	}

	cmd.log.Debug().
		Uint64("size", size).
		Uint64("created", nCreated).
		Msg("cmd/keypair generated")

	cmd.pubs = pubs

	return nil
}

func (cmd *cliControl) generateOps() error {
	if err := valkeyFlushAll(cmd.ctx, cmd.dbClient); err != nil {
		return err
	}

	if err := cmd.generateCreateOps(); err != nil {
		return err
	}

	if err := cmd.generateTransferOps(); err != nil {
		return err
	}

	return nil
}

func (cmd *cliControl) generateCreateOps() error {
	cmd.ophs = make([][2]util.Hash, cmd.NOps)

	nCreated, facts, err := generateControlItems(
		cmd.ctx,
		filepath.Join(cmd.rootDir, "ops-create.b"),
		cmd.NOps,
		func(i uint64) (dop5.OperationFact, []byte, error) {
			fact := dop5.NewAccountOpFact(
				[]byte("c"),
				1,
				nil,
				cmd.pubs[i],
			)
			cmd.ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			b, err := cmd.enc.Marshal(fact)

			return fact, b, err
		},
		func(b []byte, i uint64) (dop5.OperationFact, error) {
			var fact dop5.OperationFact
			if err := encoder.Unmarshal(cmd.enc, b, &fact); err != nil {
				return nil, err
			}

			cmd.ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			return fact, nil
		},
	)
	if err != nil {
		return err
	}

	cmd.log.Debug().
		Uint64("size", cmd.NOps).
		Uint64("created", nCreated).
		Msg("cmd/ create ops generated")

	return func() error {
		defer dutil.ElapsedLogMsg(func() *zerolog.Event {
			return cmd.log.Debug().Int("n", len(facts)) //nolint:zerologlint
		}, "cmd/ create op facts saved")()

		return valkeySaveOperationFacts(cmd.ctx, cmd.dbClient, cmd.enc, 1<<9, 1<<9, facts) //nolint:mnd
	}()
}

func (cmd *cliControl) generateTransferOps() error {
	cmd.ophs = make([][2]util.Hash, cmd.NOps)

	nCreated, facts, err := generateControlItems(
		cmd.ctx,
		filepath.Join(cmd.rootDir, "ops-transfer.b"),
		cmd.NOps,
		func(i uint64) (dop5.OperationFact, []byte, error) {
			fact := dop5.NewAccountOpFact(
				[]byte("c"),
				1,
				cmd.pubs[i],
				cmd.pubs[i+1],
			)
			cmd.ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			b, err := cmd.enc.Marshal(fact)

			return fact, b, err
		},
		func(b []byte, i uint64) (dop5.OperationFact, error) {
			var fact dop5.OperationFact
			if err := encoder.Unmarshal(cmd.enc, b, &fact); err != nil {
				return nil, err
			}

			cmd.ophs[i] = [2]util.Hash{fact.Hash(), fact.Hash()}

			return fact, nil
		},
	)
	if err != nil {
		return err
	}

	cmd.log.Debug().
		Uint64("size", cmd.NOps).
		Uint64("created", nCreated).
		Msg("cmd/ transfer ops generated")

	return func() error {
		defer dutil.ElapsedLogMsg(func() *zerolog.Event {
			return cmd.log.Debug().Int("n", len(facts)) //nolint:zerologlint
		}, "cmd/ transfer facts saved")()

		return valkeySaveOperationFacts(cmd.ctx, cmd.dbClient, cmd.enc, 1<<9, 1<<9, facts) //nolint:mnd
	}()
}

func (cmd *cliControl) launch() error {
	newJob, jobDone, jobWait := dop7.NewFuncNumCPUJobWorker(cmd.ctx)

	if err := newJob(func(context.Context) error {
		return cmd.launchManager()
	}); err != nil {
		return err
	}

	for i := range cmd.Workers {
		if err := newJob(func(context.Context) error {
			return cmd.launchWorker(i)
		}); err != nil {
			return err
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return err
	}

	return nil
}

func (cmd *cliControl) launchManager() error {
	return cmd.launchProcess("manager", nil)
}

func (cmd *cliControl) launchWorker(worker uint64) error {
	return cmd.launchProcess(fmt.Sprintf("worker-%02d", worker), []string{
		fmt.Sprintf("MITUM_DOP7_WORKER_ID=%d", worker),
	})
}

func (cmd *cliControl) launchProcess(name string, envs []string) error {
	cmd.log.Debug().Str("name", name).Msg("launch process")

	flogstdout, flogstderr := name+"-stdout.log", name+"-stderr.log"

	var logstdout, logstderr io.WriteCloser

	switch i, err := os.Create(filepath.Join(cmd.rootDir, flogstdout)); {
	case err != nil:
		return errors.Wrap(err, name+": process stdout log")
	default:
		logstdout = i
	}

	switch i, err := os.Create(filepath.Join(cmd.rootDir, flogstderr)); {
	case err != nil:
		return errors.Wrap(err, name+": process stderr log")
	default:
		logstderr = i
	}

	c := exec.CommandContext(cmd.ctx, cmd.launchProg)
	c.Env = append(os.Environ(),
		"MITUM_DOP7_NODE_TYPE="+name,
		"MITUM_DOP7_CONTROL_URL=192.168.54.3:12340", // FIXME set control url
	)
	c.Env = append(os.Environ(), envs...)

	var stdout, stderr io.ReadCloser
	switch i, err := c.StdoutPipe(); {
	case err != nil:
		return err
	default:
		stdout = i
	}

	switch i, err := c.StderrPipe(); {
	case err != nil:
		return err
	default:
		stderr = i
	}

	go func() {
		_, err := io.Copy(logstdout, stdout)
		cmd.log.Debug().Err(err).Str("name", name).Msg("process stdout")
	}()

	go func() {
		_, err := io.Copy(logstderr, stderr)
		cmd.log.Debug().Err(err).Str("name", name).Msg("process stderr")
	}()

	if err := c.Start(); err != nil {
		return err
	}

	go func() {
		defer func() {
			_ = stdout.Close()
			_ = stderr.Close()
			_ = logstdout.Close()
			_ = logstderr.Close()
		}()

		err := c.Wait()
		cmd.log.Debug().Err(err).Str("name", name).Msg("process stopped")
		if err != nil {
			_ = cmd.ctxCancel(err)
		}
	}()

	return nil
}

func generateControlItems[T any](
	ctx context.Context,
	filename string,
	size uint64,
	encode func(uint64) (T, []byte, error),
	load func([]byte, uint64) (T, error),
) (uint64, []T, error) {
	items := make([]T, size)
	rem := size

	var prev *os.File

	switch f, err := os.Open(filepath.Clean(filename)); {
	case err != nil && !os.IsNotExist(err):
		return 0, nil, errors.WithStack(err)
	case os.IsNotExist(err):
	default:
		defer func() { _ = f.Close() }()

		prev = f

		p, err := loadControlItems(ctx, f, size, load)

		switch {
		case err != nil:
			return 0, nil, err
		case len(p) > 0:
			copy(items, p)

			rem = size - uint64(len(p))
		}
	}

	if rem < 1 {
		return 0, items, nil
	}

	var w *os.File

	switch f, err := os.Create(filepath.Clean(filename + ".tmp")); {
	case err != nil:
		return 0, nil, errors.WithStack(err)
	default:
		if err := dutil.WriteInt(f, size); err != nil {
			return 0, nil, err
		}

		if prev != nil {
			if _, err := prev.Seek(0, 0); err != nil {
				return 0, nil, errors.WithStack(err)
			}

			var nsize uint64
			if err := dutil.ReadInt(prev, &nsize); err != nil {
				return 0, nil, err
			}

			if _, err := io.Copy(f, prev); err != nil {
				return 0, nil, errors.WithStack(err)
			}

			_ = prev.Close()
		}

		w = f
	}

	newJob, jobDone, jobWait := dop7.NewFuncNumCPUJobWorker(ctx)

	for i := range rem {
		if err := newJob(func(context.Context) error {
			buf := bufferPool.Get().(*bytes.Buffer)
			defer bufferPoolPut(buf)

			index := size - rem + i

			if _, err := frame.WriteUint64(buf, index); err != nil {
				return err
			}

			switch v, b, err := encode(index); {
			case err != nil:
				return err
			default:
				items[index] = v

				if _, err := frame.WriteBytes(buf, b); err != nil {
					return err
				}
			}

			if _, err := io.Copy(w, buf); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return 0, nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return 0, nil, err
	}

	_ = w.Close()

	if err := os.Rename(filename+".tmp", filename); err != nil {
		return 0, nil, errors.WithStack(err)
	}

	return rem, items, nil
}

func loadControlItems[T any](
	ctx context.Context,
	r io.Reader,
	limit uint64,
	load func([]byte, uint64) (T, error),
) ([]T, error) {
	var size uint64
	if err := dutil.ReadInt(r, &size); err != nil {
		return nil, err
	}

	nsize := size
	if size > limit {
		nsize = limit
	}

	items := make([]T, nsize)

	newJob, jobDone, jobWait := dop7.NewFuncNumCPUJobWorker(ctx)

	var count uint64
	for count < nsize {
		index, b, err := readIndexItem(r)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}

		if index >= nsize {
			continue
		}

		count++

		if err := newJob(func(context.Context) error {
			switch v, err := load(b, index); {
			case err != nil:
				return err
			default:
				items[index] = v

				return nil
			}
		}); err != nil {
			return nil, jobWait(err)
		}
	}

	jobDone()

	if err := jobWait(nil); err != nil {
		return nil, err
	}

	return items, nil
}

func readIndexItem(r io.Reader) (uint64, []byte, error) { // FIXME remove
	switch index, _, err := frame.ReadUint64(r); {
	case err != nil:
		return 0, nil, err
	default:
		_, br, err := frame.ReadBytes(r)
		if err != nil {
			return 0, nil, err
		}

		b, err := io.ReadAll(br)

		return index, b, err
	}
}

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(nil)
	},
}

func bufferPoolPut(buf *bytes.Buffer) {
	buf.Reset()

	bufferPool.Put(buf)
}
