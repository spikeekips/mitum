package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/isaac"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/hint"
)

var LocalFSWriterHint = hint.MustNewHint("block-localfs-writer-364-v0.0.1")

type (
	ioWriteFunc                     func([]byte) (int, error)
	blockFileDoneFileFunc           func(_ error, filenameBody string) (filename string, _ error)
	blockFileWriterCreateWriterFunc func(_ base.BlockItemType, createID string) (
		compressFormat string,
		_ ioWriteFunc,
		_ blockFileDoneFileFunc,
		_ error,
	)
	blockFileChunkWriterBaseNewWriteFunc  func(header interface{}, _ func(ioWriteFunc) error) (blockFileChunk, error)
	blockFileChunkWriterBaseNewWriteFuncs func(createID string) (blockFileChunkWriterBaseNewWriteFunc, error)
)

func newFuncBaseBlockFileChunkWriter(
	it base.BlockItemType,
	createWriter blockFileWriterCreateWriterFunc,
) blockFileChunkWriterBaseNewWriteFuncs {
	return func(createID string) (blockFileChunkWriterBaseNewWriteFunc, error) {
		if len(createID) < 1 {
			return nil, errors.Errorf("empty create id")
		}

		return func(header interface{}, f func(ioWriteFunc) error) (blockFileChunk, error) {
				var compressFormat string
				checksumf := func() string { return "" }
				var write ioWriteFunc

				donef := util.EmptyLocked[blockFileDoneFileFunc]()

				done := func(err error, filenameBody string) (string, error) {
					_ = checksumf()

					if f, isempty := donef.Value(); !isempty {
						return f(err, filenameBody)
					}

					return "", err
				}

				closef := func(err error) error {
					_, err = done(err, "")

					return err
				}

				once := OnceFunc(func() error {
					switch c, w, d, err := createWriter(it, createID); {
					case err != nil:
						return err
					default:
						write, checksumf = newChecksumWrite(w)

						compressFormat = c
						donef.SetValue(d)
					}

					if header != nil {
						if err := writeBlockFileFileHeader(write, header); err != nil {
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
					return nil, closef(err)
				}

				checksum := checksumf()
				if len(checksum) < 1 {
					return nil, closef(nil)
				}

				switch filename, err := done(nil, blockFileFilenameBody(it, checksum)); {
				case err != nil:
					return nil, err
				default:
					return newBaseBlockFileChunk(filename, checksum, compressFormat), nil
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

func encodeBlockFileFileHeader(hs interface{}) ([]byte, error) {
	if hs == nil {
		return nil, nil
	}

	return util.MarshalJSON(hs)
}

func writeBlockFileFileHeader(write ioWriteFunc, hs interface{}) error {
	if hs == nil {
		return nil
	}

	var b []byte

	switch hs.(type) {
	case []byte:
		b = hs.([]byte)
	case json.RawMessage:
		b = hs.(json.RawMessage)
	default:
		i, err := encodeBlockFileFileHeader(hs)
		if err != nil {
			return err
		}

		b = i
	}

	var s strings.Builder
	_, _ = s.WriteString("# ")
	_, _ = s.WriteString(string(b))
	_, _ = s.WriteString("\n")

	if _, err := write([]byte(s.String())); err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func readBlockFileFileHeader(r io.Reader, v interface{}) (*bufio.Reader, error) {
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

func safeStreamerEncode(enc util.StreamEncoder) func(interface{}) error {
	var l sync.Mutex

	return func(i interface{}) error {
		l.Lock()
		defer l.Unlock()

		return enc.Encode(i)
	}
}

func blockFileFilenameBody(it base.BlockItemType, checksum string) string {
	var pck string

	if len(checksum) > 0 {
		pck = "-" + checksum
	}

	return it.String() + pck
}

func newBlockFileMultiChunksWriter(
	ctx context.Context,
	chunkItems, maxSemSize, total uint64,
	header interface{},
	w func() (blockFileChunkWriterBaseNewWriteFunc, error),
	f func(func(func(ioWriteFunc) error) error) error,
) ([]blockFileChunk, error) {
	switch {
	case total < 1:
		return nil, nil
	case chunkItems < 1:
		return nil, util.ErrInvalid.Errorf("zero chunkItems")
	case maxSemSize < 1:
		return nil, util.ErrInvalid.Errorf("zero maxSemSize")
	}

	if chunkItems > total {
		chunkItems = total
	}

	var jw JobWorker

	semSize := total / chunkItems
	if total%chunkItems > 0 {
		semSize++
	}

	chunksSize := semSize + 1

	if semSize > maxSemSize {
		semSize = maxSemSize
	}

	var headerb json.RawMessage

	switch i, err := encodeBlockFileFileHeader(header); {
	case err != nil:
		return nil, err
	default:
		headerb = i
	}

	wctx, wcancel := ContextWithCancelCause(ctx)
	defer wcancel(nil)

	switch i, err := NewBaseJobWorker(wctx, int64(semSize)); {
	case err != nil:
		return nil, err
	default:
		defer i.Close()

		jw = i
	}

	var addChunk util.CompactAppendSliceAddFunc[blockFileChunk]
	var doneChunk util.CompactAppendSliceDoneFunc[blockFileChunk]

	switch a, d, err := util.CompactAppendSliceV1[blockFileChunk](chunksSize); {
	case err != nil:
		return nil, err
	default:
		addChunk = a
		doneChunk = d
	}

	errorf := func(err error) error {
		wcancel(err)
		doneChunk(nil)

		return err
	}

	buckets := make([]chan func(ioWriteFunc) error, semSize)
	bucketsCount := make([]uint64, semSize)
	defer func() {
		clear(buckets)
		clear(bucketsCount)
	}()

	newBucket := func(b uint64) error {
		if buckets[b] != nil {
			close(buckets[b])
		}

		ch := make(chan func(ioWriteFunc) error, chunkItems)
		buckets[b] = ch
		bucketsCount[b] = 0

		return jw.NewJob(func(ctx context.Context) error {
			switch chunk, err := runBucketBlockFileMultiChunksWriter(
				ctx,
				chunkItems,
				headerb,
				w,
				ch,
			); {
			case err != nil:
				return err
			default:
				if chunk != nil {
					addChunk(chunk)
				}

				return nil
			}
		})
	}

	for i := uint64(0); i < semSize; i++ {
		if err := newBucket(i); err != nil {
			return nil, errorf(err)
		}
	}

	var count uint64

	if err := f(func(c func(ioWriteFunc) error) error {
		b := count % semSize

		defer func() {
			bucketsCount[b]++
			count++

			if bucketsCount[b] == chunkItems {
				if err := newBucket(b); err != nil {
					errorf(err)
				}
			}
		}()

		buckets[b] <- c

		return nil
	}); err != nil {
		return nil, errorf(err)
	}

	_ = util.TraverseSliceV1(buckets, func(_ int, ch chan func(ioWriteFunc) error) (bool, error) {
		close(ch)

		return true, nil
	})

	jw.JobDone()

	if err := jw.Wait(); err != nil {
		return nil, errorf(err)
	}

	var chunks []blockFileChunk

	_ = doneChunk(func(s []blockFileChunk) error {
		chunks = slices.Clone(s)

		return nil
	})

	return chunks, nil
}

func runBucketBlockFileMultiChunksWriter(
	ctx context.Context,
	chunkItems uint64,
	header interface{},
	w func() (blockFileChunkWriterBaseNewWriteFunc, error),
	ch chan func(ioWriteFunc) error,
) (blockFileChunk, error) {
	var c blockFileChunkWriterBaseNewWriteFunc

	var jw JobWorker

	switch i, err := NewBaseJobWorker(ctx, int64(chunkItems)); {
	case err != nil:
		return nil, err
	default:
		defer i.Close()

		jw = i
	}

	switch i, err := w(); {
	case err != nil:
		return nil, err
	default:
		c = i
	}

	return c(header, func(write ioWriteFunc) error {
	end:
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case j, notClosed := <-ch:
				if !notClosed {
					break end
				}

				if err := jw.NewJob(func(context.Context) error {
					return j(write)
				}); err != nil {
					return err
				}
			}
		}

		jw.JobDone()

		return jw.Wait()
	})
}
