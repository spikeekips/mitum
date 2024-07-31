package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	isaacblock "github.com/spikeekips/mitum/isaac/block"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var LocalFSWriterHint = hint.MustNewHint("block-localfs-writer-364-v0.0.1")

type (
	ioWriteFunc                     func([]byte) (int, error)
	blockItemWriterCreateWriterFunc func(_ base.BlockItemType, createID string) (
		ioWriteFunc,
		func(error) error,
		error,
	)
	blockItemWriterBaseNewWriteFunc  func(header interface{}, _ func(ioWriteFunc) error) (base.BlockMapItem, error)
	blockItemWriterBaseNewWriteFuncs func(createID string) (blockItemWriterBaseNewWriteFunc, error)
)

func newBaseBlockItemWriterFunc(
	it base.BlockItemType,
	createWriter blockItemWriterCreateWriterFunc,
) blockItemWriterBaseNewWriteFuncs {
	return func(createID string) (blockItemWriterBaseNewWriteFunc, error) {
		if len(createID) < 1 {
			return nil, errors.Errorf("empty create id")
		}

		return func(header interface{}, f func(ioWriteFunc) error) (base.BlockMapItem, error) {
				done := func(err error) error { return err }
				checksum := func() string { return "" }
				headerOnce := func() error { return nil }

				var write ioWriteFunc

				once := OnceFunc(func() error {
					switch w, d, err := createWriter(it, createID); {
					case err != nil:
						return err
					default:
						h := sha256.New()
						defer h.Reset()

						write, checksum = newChecksumWriteFuncs(w, h)
						done = d

						if header != nil {
							headerOnce = OnceFunc(func() error {
								return writeBlockItemFileHeader(write, header)
							})
						}

						return nil
					}
				})

				if err := f(func(b []byte) (int, error) {
					if err := once(); err != nil {
						return 0, err
					}

					if err := headerOnce(); err != nil {
						return 0, err
					}

					return write(b)
				}); err != nil {
					return nil, done(err)
				}

				if err := done(nil); err != nil {
					return nil, err
				}

				switch ck := checksum(); {
				case len(ck) > 0:
					return isaacblock.NewBlockMapItem(it, ck), nil
				default:
					return nil, nil
				}
			},
			nil
	}
}

type totalItemsHeader struct {
	isaac.BlockItemFileBaseItemsHeader
	Total uint64 `json:"total"`
}

type treeItemsHeader struct {
	Tree hint.Hint `json:"tree"`
	totalItemsHeader
}

func newItemsHeader(enc hint.Hint) isaac.BlockItemFileBaseItemsHeader {
	return isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: enc}
}

func newTotalItemsHeader(enc hint.Hint, total uint64) totalItemsHeader {
	return totalItemsHeader{
		BlockItemFileBaseItemsHeader: isaac.BlockItemFileBaseItemsHeader{Writer: LocalFSWriterHint, Encoder: enc},
		Total:                        total,
	}
}

func newTreeItemsHeader(enc hint.Hint, total uint64, tree hint.Hint) treeItemsHeader {
	return treeItemsHeader{
		totalItemsHeader: newTotalItemsHeader(enc, total),
		Tree:             tree,
	}
}

func writeBlockItemFileHeader(write ioWriteFunc, hs interface{}) error {
	switch b, err := util.MarshalJSON(hs); {
	case err != nil:
		return err
	default:
		var s strings.Builder
		_, _ = s.WriteString("# ")
		_, _ = s.WriteString(string(b))
		_, _ = s.WriteString("\n")

		if _, err := write([]byte(s.String())); err != nil {
			return errors.WithStack(err)
		}

		return nil
	}
}

func readBlockItemFileHeader(r io.Reader, v interface{}) (*bufio.Reader, error) {
	switch bf, b, err := isaac.ReadBlockItemFileHeader(r); {
	case err != nil:
		return nil, err
	default:
		if err := util.UnmarshalJSON(b, &v); err != nil {
			return nil, err
		}

		return bf, nil
	}
}

type writeFunc func([]byte) (int, error)

func (w writeFunc) Write(p []byte) (int, error) {
	return w(p)
}

func newChecksumWriteFuncs(write ioWriteFunc, h hash.Hash) (
	ioWriteFunc,
	func() string,
) {
	var l sync.RWMutex

	w := io.MultiWriter(writeFunc(write), h)

	return func(b []byte) (int, error) {
			l.Lock()
			defer l.Unlock()

			return w.Write(b)
		},
		func() string {
			l.RLock()
			defer l.RUnlock()

			switch s := h.Sum(nil); {
			case len(s) < 1:
				return ""
			default:
				return hex.EncodeToString(s)
			}
		}
}

func safeStreamerEncode(enc util.StreamEncoder) func(interface{}) error {
	var l sync.Mutex

	return func(i interface{}) error {
		l.Lock()
		defer l.Unlock()

		return enc.Encode(i)
	}
}
