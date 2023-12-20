package isaacblock

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type LocalFSImporter struct {
	root   string
	enc    encoder.Encoder
	bfiles *isaac.BlockItemFilesMaker
	temp   string
	height base.Height
}

func NewLocalFSImporter(root string, jsonenc, enc encoder.Encoder, m base.BlockMap) (*LocalFSImporter, error) {
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, errors.Wrap(err, "new LocalFSImporter")
	}

	temp := filepath.Join(
		abs,
		BlockTempDirectoryPrefix,
		fmt.Sprintf("%d-%s", m.Manifest().Height(), util.ULID().String()),
	)

	if err := os.MkdirAll(temp, 0o700); err != nil {
		return nil, errors.Wrap(err, "create temp directory")
	}

	return &LocalFSImporter{
		root:   abs,
		enc:    enc,
		temp:   temp,
		bfiles: isaac.NewBlockItemFilesMaker(jsonenc),
	}, nil
}

// WriteMap writes BlockMap; BlockMap should be already sign.
func (l *LocalFSImporter) WriteMap(m base.BlockMap) error {
	e := util.StringError("write map to local fs")

	var w io.WriteCloser

	switch i, err := DefaultBlockItemFileName(base.BlockItemMap, l.enc.Hint().Type()); {
	case err != nil:
		return e.Wrap(err)
	default:
		if _, err := l.bfiles.SetItem(base.BlockItemMap, isaac.NewLocalFSBlockItemFile(i, "")); err != nil {
			return err
		}

		j, err := l.newWriter(i)
		if err != nil {
			return e.Wrap(err)
		}

		defer func() {
			_ = j.Close()
		}()

		w = j
	}

	if err := writeBaseHeader(w,
		isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: l.enc.Hint()},
	); err != nil {
		return e.Wrap(err)
	}

	if err := util.PipeReadWrite(
		context.Background(),
		func(_ context.Context, pr io.Reader) error {
			_, err := io.Copy(w, pr)

			return errors.WithStack(err)
		},
		func(_ context.Context, pw io.Writer) error {
			return l.enc.StreamEncoder(pw).Encode(m)
		},
	); err != nil {
		return e.Wrap(err)
	}

	l.height = m.Manifest().Height()

	return nil
}

func (l *LocalFSImporter) WriteItem(
	t base.BlockItemType,
	enc hint.Hint,
	compressFormat string,
) (io.WriteCloser, error) {
	e := util.StringError("write item to local fs")

	f, err := BlockItemFileName(t, enc.Type(), compressFormat)
	if err != nil {
		return nil, e.Wrap(err)
	}

	if _, err := l.bfiles.SetItem(t, isaac.NewLocalFSBlockItemFile(f, "")); err != nil {
		return nil, err
	}

	return l.newWriter(f)
}

func (l *LocalFSImporter) Save() error {
	heightdirectory := filepath.Join(l.root, isaac.BlockHeightDirectory(l.height))

	if err := l.save(heightdirectory); err != nil {
		_ = os.RemoveAll(heightdirectory)

		return errors.WithMessage(err, "save local fs")
	}

	return nil
}

func (l *LocalFSImporter) save(heightdirectory string) error {
	switch _, err := os.Stat(heightdirectory); {
	case err == nil:
		if err = os.RemoveAll(heightdirectory); err != nil {
			return errors.WithMessage(err, "remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return errors.WithMessage(err, "check height directory")
	}

	switch err := os.MkdirAll(filepath.Dir(heightdirectory), 0o700); {
	case err == nil:
	case os.IsExist(err):
	default:
		return errors.WithMessage(err, "create height parent directory")
	}

	if err := os.Rename(l.temp, heightdirectory); err != nil {
		return errors.WithStack(err)
	}

	return l.bfiles.Save(isaac.BlockItemFilesPath(l.root, l.height))
}

func (l *LocalFSImporter) Cancel() error {
	e := util.StringError("cancel local fs")

	switch _, err := os.Stat(l.temp); {
	case err == nil:
		if err = os.RemoveAll(l.temp); err != nil {
			return e.WithMessage(err, "remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return e.WithMessage(err, "check temp directory")
	}

	d := filepath.Join(l.root, isaac.BlockHeightDirectory(l.height))

	switch _, err := os.Stat(d); {
	case err == nil:
		if err = os.RemoveAll(d); err != nil {
			return e.WithMessage(err, "remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return e.WithMessage(err, "check height directory")
	}

	return nil
}

func (l *LocalFSImporter) newWriter(filename string) (io.WriteCloser, error) {
	f, err := os.OpenFile(filepath.Join(l.temp, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)

	return f, errors.WithStack(err)
}
