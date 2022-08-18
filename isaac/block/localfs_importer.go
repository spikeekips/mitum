package isaacblock

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
)

type LocalFSImporter struct {
	root   string
	enc    encoder.Encoder
	temp   string
	height base.Height
}

func NewLocalFSImporter(root string, enc encoder.Encoder, m base.BlockMap) (*LocalFSImporter, error) {
	abs, err := filepath.Abs(filepath.Clean(root))
	if err != nil {
		return nil, errors.Wrap(err, "failed new LocalFSImporter")
	}

	temp := filepath.Join(
		abs,
		BlockTempDirectoryPrefix,
		fmt.Sprintf("%d-%s", m.Manifest().Height(), util.ULID().String()),
	)

	if err := os.MkdirAll(temp, 0o700); err != nil {
		return nil, errors.Wrap(err, "failed to create temp directory")
	}

	return &LocalFSImporter{
		root: abs,
		enc:  enc,
		temp: temp,
	}, nil
}

// WriteMap writes BlockMap; BlockMap should be already signed.
func (l *LocalFSImporter) WriteMap(m base.BlockMap) error {
	e := util.StringErrorFunc("failed to write map to localfs")

	var r io.Reader

	switch b, err := l.enc.Marshal(m); {
	case err != nil:
		return e(err, "")
	default:
		r = bytes.NewBuffer(b)
	}

	w, err := l.newWriter(blockFSMapFilename(l.enc.Hint().Type().String()))
	if err != nil {
		return e(err, "")
	}

	if _, err := io.Copy(w, r); err != nil {
		return e(err, "")
	}

	l.height = m.Manifest().Height()

	return nil
}

func (l *LocalFSImporter) WriteItem(t base.BlockMapItemType) (io.WriteCloser, error) {
	e := util.StringErrorFunc("failed to write item to localfs")

	f, err := BlockFileName(t, l.enc.Hint().Type().String())
	if err != nil {
		return nil, e(err, "")
	}

	return l.newWriter(f)
}

func (l *LocalFSImporter) Save() error {
	e := util.StringErrorFunc("failed to save localfs")

	d := filepath.Join(l.root, HeightDirectory(l.height))

	switch _, err := os.Stat(d); {
	case err == nil:
		if err = os.RemoveAll(d); err != nil {
			return e(err, "failed to remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return e(err, "failed to check height directory")
	}

	switch err := os.MkdirAll(filepath.Dir(d), 0o700); {
	case err == nil:
	case os.IsExist(err):
	default:
		return e(err, "failed to create height parent directory")
	}

	if err := os.Rename(l.temp, d); err != nil {
		return e(err, "")
	}

	return nil
}

func (l *LocalFSImporter) Cancel() error {
	e := util.StringErrorFunc("failed to cancel localfs")

	switch _, err := os.Stat(l.temp); {
	case err == nil:
		if err = os.RemoveAll(l.temp); err != nil {
			return e(err, "failed to remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return e(err, "failed to check temp directory")
	}

	d := filepath.Join(l.root, HeightDirectory(l.height))

	switch _, err := os.Stat(d); {
	case err == nil:
		if err = os.RemoveAll(d); err != nil {
			return e(err, "failed to remove existing height directory")
		}
	case os.IsNotExist(err):
	default:
		return e(err, "failed to check height directory")
	}

	return nil
}

func (l *LocalFSImporter) newWriter(filename string) (io.WriteCloser, error) {
	f, err := os.OpenFile(filepath.Join(l.temp, filename), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)

	return f, errors.WithStack(err)
}
