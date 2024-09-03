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
	ioWriteFunc                        func([]byte) (int, error)
	blockMapItemDoneFileFunc           func(_ error, filenameBody string) error
	blockMapItemWriterCreateWriterFunc func(_ base.BlockItemType, createID string) (
		compressFormat string,
		_ ioWriteFunc,
		_ blockMapItemDoneFileFunc,
		_ error,
	)
	blockMapItemWriterBaseNewWriteFunc       func(header interface{}, _ func(ioWriteFunc) error) (base.BlockMapItem, error)
	blockMapItemWriterBaseNewWriteFuncs      func(createID string) (blockMapItemWriterBaseNewWriteFunc, error)
	blockMapItemChunkWriterBaseNewWriteFunc  func(header interface{}, _ func(ioWriteFunc) error) (blockMapItemChunk, error)
	blockMapItemChunkWriterBaseNewWriteFuncs func(createID string) (blockMapItemChunkWriterBaseNewWriteFunc, error)
)

func newFuncBaseBlockMapItemChunkWriter(
	it base.BlockItemType,
	createWriter blockMapItemWriterCreateWriterFunc,
) blockMapItemChunkWriterBaseNewWriteFuncs {
	return func(createID string) (blockMapItemChunkWriterBaseNewWriteFunc, error) {
		if len(createID) < 1 {
			return nil, errors.Errorf("empty create id")
		}

		return func(header interface{}, f func(ioWriteFunc) error) (blockMapItemChunk, error) {
				donef := util.EmptyLocked[blockMapItemDoneFileFunc]()

				done := func(err error, filenameBody string) error {
					if f, isempty := donef.Value(); !isempty {
						return f(err, filenameBody)
					}

					return err
				}

				var compressFormat string
				checksumf := func() string { return "" }
				var write ioWriteFunc

				once := OnceFunc(func() error {
					switch c, w, d, err := createWriter(it, createID); {
					case err != nil:
						return err
					default:
						w, chk := newChecksumWrite(w, sha256.New())

						write = w
						compressFormat = c
						checksumf = chk
						donef.SetValue(d)
					}

					if header != nil {
						if err := writeBlockMapItemFileHeader(write, header); err != nil {
							return err
						}
					}

					return nil
				})

				if err := f(func(b []byte) (int, error) {
					if err := once(); err != nil {
						return 0, err
					}

					return write(b)
				}); err != nil {
					return nil, done(err, "")
				}

				checksum := checksumf()
				if len(checksum) < 1 {
					return nil, done(nil, "")
				}

				filenameBody := blockMapItemFilenameBody(it, checksum)
				if err := done(nil, filenameBody); err != nil {
					return nil, err
				}

				return newBaseBlockMapItemChunk(filenameBody, checksum, compressFormat), nil
			},
			nil
	}
}

func newFuncBaseBlockMapItemWriter(
	it base.BlockItemType,
	createWriter blockMapItemWriterCreateWriterFunc,
) blockMapItemWriterBaseNewWriteFuncs {
	return func(createID string) (blockMapItemWriterBaseNewWriteFunc, error) {
		if len(createID) < 1 {
			return nil, errors.Errorf("empty create id")
		}

		return func(header interface{}, f func(ioWriteFunc) error) (base.BlockMapItem, error) {
				donef := util.EmptyLocked[blockMapItemDoneFileFunc]()

				done := func(err error, filenameBody string) error {
					if f, isempty := donef.Value(); !isempty {
						return f(err, filenameBody)
					}

					return err
				}

				checksumf := func() string { return "" }
				var write ioWriteFunc

				once := OnceFunc(func() error {
					switch _, w, d, err := createWriter(it, createID); {
					case err != nil:
						return err
					default:
						w, chk := newChecksumWrite(w, sha256.New())

						write = w
						checksumf = chk
						donef.SetValue(d)
					}

					if header != nil {
						if err := writeBlockMapItemFileHeader(write, header); err != nil {
							return err
						}
					}

					return nil
				})

				if err := f(func(b []byte) (int, error) {
					if err := once(); err != nil {
						return 0, err
					}

					return write(b)
				}); err != nil {
					return nil, done(err, "")
				}

				checksum := checksumf()
				if len(checksum) < 1 {
					return nil, done(nil, "")
				}

				filenameBody := blockMapItemFilenameBody(it, checksum)

				if err := done(nil, filenameBody); err != nil {
					return nil, err
				}

				return isaacblock.NewBlockMapItem(it, checksum), nil
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

func writeBlockMapItemFileHeader(write ioWriteFunc, hs interface{}) error {
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

func readBlockMapItemFileHeader(r io.Reader, v interface{}) (*bufio.Reader, error) {
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

func newChecksumWrite(write ioWriteFunc, h hash.Hash) (
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
			l.Lock()
			defer l.Unlock()

			defer h.Reset()

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

func blockMapItemFilenameBody(it base.BlockItemType, checksum string) string {
	var pck string

	if len(checksum) > 0 {
		pck = "-" + checksum
	}

	return it.String() + pck
}
