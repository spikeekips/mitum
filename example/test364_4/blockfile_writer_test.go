package main

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
	"go.uber.org/goleak"
)

type baseTestBaseBlockFileWriter struct {
	baseTestWorkerMerger
	enc encoder.Encoder
}

func (t *baseTestBaseBlockFileWriter) SetupSuite() {
	t.baseTestWorkerMerger.Suite = t.Suite
	t.baseTestWorkerMerger.SetupSuite()

	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: AccountStateValueHint, Instance: AccountStateValue{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: AccountStateMergeValueHint, Instance: AccountStateMergeValue{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: OpFactHint, Instance: OpFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: OpHint, Instance: Op{}}))
}

func (t *baseTestBaseBlockFileWriter) newCreateWriter() (
	blockFileWriterCreateWriterFunc,
	func(func([][]byte) error) error,
) {
	add, done, err := util.CompactAppendSliceV1[*bytes.Buffer](1<<9, 1<<9)
	t.NoError(err)

	return func(base.BlockItemType, string) (string, ioWriteFunc, blockFileDoneFileFunc, error) {
			w := bytes.NewBuffer(nil)
			add(w)

			return "", w.Write, func(err error, _ string) (string, error) { return util.UUID().String(), err }, nil
		},
		func(f func([][]byte) error) error {
			return done(func(s []*bytes.Buffer) error {
				u := make([][]byte, len(s))
				defer func() {
					clear(u)

					for i := range s {
						s[i].Reset()
					}
				}()

				for i := range s {
					u[i] = s[i].Bytes()
				}

				return f(u)
			})
		}
}

func (t *baseTestBaseBlockFileWriter) compareChecksum(input []byte, output string) bool {
	write, done := newChecksumWrite(
		func(b []byte) (int, error) { return len(b), nil },
	)

	_, err := write(input)
	t.NoError(err, "%+v", err)

	return done() == output
}

type testBlockFileMultiChunksWriter struct {
	suite.Suite
}

func (t *testBlockFileMultiChunksWriter) testWriter(
	ctx context.Context,
	chunkItems, maxSemSize, total, expectedChunksSize uint64,
) {
	t.T().Helper()

	if chunkItems > total {
		chunkItems = total
	}

	items := make([]string, total)
	for i := range items {
		items[i] = fmt.Sprintf("%02d", i)
	}

	var addB util.CompactAppendSliceAddFunc[string]
	var doneB util.CompactAppendSliceDoneFunc[string]

	addB, doneB, err := util.CompactAppendSliceV1[string](1<<9, 1<<9)
	t.NoError(err)

	expectedChunks := make([]blockFileChunk, expectedChunksSize)

	for i := range expectedChunks {
		expectedChunks[i] = newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), "")
	}

	slices.SortFunc(expectedChunks, func(a, b blockFileChunk) int {
		return strings.Compare(a.URI(), b.URI())
	})

	var chunkIndex uint64

	chunks, err := newBlockFileMultiChunksWriter(
		ctx,
		chunkItems,
		maxSemSize,
		total,
		nil,
		func() (blockFileChunkWriterBaseNewWriteFunc, error) {
			return func(header interface{}, f func(ioWriteFunc) error) (blockFileChunk, error) {
					c := atomic.AddUint64(&chunkIndex, 1) - 1

					var called int64

					write := func(b []byte) (int, error) {
						atomic.AddInt64(&called, 1)

						addB(string(b))

						return len(b), nil
					}

					if err := f(write); err != nil {
						return nil, err
					}

					if atomic.LoadInt64(&called) < 1 {
						return nil, nil
					}

					return expectedChunks[c], nil
				},
				nil
		},
		func(f func(func(ioWriteFunc) error) error) error {
			for i := range items {
				i := i
				item := items[i]

				if err := f(func(write ioWriteFunc) error {
					_, err := write([]byte(item))

					return err
				}); err != nil {
					return err
				}
			}

			return nil
		},
	)
	t.NoError(err)

	doneB(func(s []string) error {
		slices.Sort(s)
		slices.Sort(items)

		t.Equal(len(items), len(s))
		t.Equal(items, s)

		return nil
	})

	slices.SortFunc(chunks, func(a, b blockFileChunk) int {
		return strings.Compare(a.URI(), b.URI())
	})

	t.Equal(len(expectedChunks), len(chunks))
	t.Equal(expectedChunks, chunks)
}

func (t *testBlockFileMultiChunksWriter) TestError() {
	t.Run("zero chunkItems", func() {
		_, err := newBlockFileMultiChunksWriter(context.Background(), 0, 3, 2, nil, nil, nil)
		t.ErrorContains(err, "zero chunkItems")
	})

	t.Run("zero maxSemSize", func() {
		_, err := newBlockFileMultiChunksWriter(context.Background(), 1, 0, 2, nil, nil, nil)
		t.ErrorContains(err, "zero maxSemSize")
	})
}

func (t *testBlockFileMultiChunksWriter) TestCancel() {
	t.Run("cancel before new", func() {
		ctx, cancel := context.WithCancel(context.Background())

		cancel()

		var total uint64 = 33
		var chunkItems uint64 = 3
		var maxSemSize uint64 = 3

		chunks, err := newBlockFileMultiChunksWriter(
			ctx,
			chunkItems,
			maxSemSize,
			total,
			nil,
			func() (blockFileChunkWriterBaseNewWriteFunc, error) {
				return func(header interface{}, f func(ioWriteFunc) error) (blockFileChunk, error) {
						return nil, nil
					},
					nil
			},
			func(f func(func(ioWriteFunc) error) error) error {
				return nil
			},
		)
		t.ErrorIs(err, context.Canceled)
		t.Empty(chunks)
	})

	t.Run("cancel during write", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var total uint64 = 33
		var chunkItems uint64 = 3
		var maxSemSize uint64 = 3

		items := make([]string, total)
		for i := range items {
			items[i] = fmt.Sprintf("%02d", i)
		}

		write := func(b []byte) (int, error) {
			return len(b), nil
		}

		var called uint64

		chunks, err := newBlockFileMultiChunksWriter(
			ctx,
			chunkItems,
			maxSemSize,
			total,
			nil,
			func() (blockFileChunkWriterBaseNewWriteFunc, error) {
				return func(header interface{}, f func(ioWriteFunc) error) (blockFileChunk, error) {
						if atomic.AddUint64(&called, 1) == 9 {
							cancel()
						}

						if err := f(write); err != nil {
							return nil, err
						}

						return newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), ""), nil
					},
					nil
			},
			func(f func(func(ioWriteFunc) error) error) error {
				for i := range items {
					i := i
					item := items[i]

					if err := f(func(write ioWriteFunc) error {
						_, err := write([]byte(item))

						return err
					}); err != nil {
						return err
					}
				}

				return nil
			},
		)
		t.ErrorIs(err, context.Canceled)
		t.Empty(chunks)
	})

	t.Run("cancel during insert", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var total uint64 = 33
		var chunkItems uint64 = 3
		var maxSemSize uint64 = 3

		items := make([]string, total)
		for i := range items {
			items[i] = fmt.Sprintf("%02d", i)
		}

		write := func(b []byte) (int, error) {
			return len(b), nil
		}

		var called uint64

		chunks, err := newBlockFileMultiChunksWriter(
			ctx,
			chunkItems,
			maxSemSize,
			total,
			nil,
			func() (blockFileChunkWriterBaseNewWriteFunc, error) {
				return func(header interface{}, f func(ioWriteFunc) error) (blockFileChunk, error) {
						if err := f(write); err != nil {
							return nil, err
						}

						return newBaseBlockFileChunk(util.UUID().String(), util.UUID().String(), ""), nil
					},
					nil
			},
			func(f func(func(ioWriteFunc) error) error) error {
				for i := range items {
					i := i
					item := items[i]

					if atomic.AddUint64(&called, 1) == 9 {
						cancel()
					}

					if err := f(func(write ioWriteFunc) error {
						_, err := write([]byte(item))

						return err
					}); err != nil {
						return err
					}
				}

				return nil
			},
		)
		t.ErrorIs(err, context.Canceled)
		t.Empty(chunks)
	})
}

func (t *testBlockFileMultiChunksWriter) TestWrite() {
	t.Run("zero total", func() {
		chunks, err := newBlockFileMultiChunksWriter(context.Background(), 1, 1, 0, nil, nil, nil)
		t.NoError(err)

		t.Empty(chunks)
	})

	t.Run("under chunkItems", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.testWriter(ctx, 3, 3, 2, 1)
	})

	t.Run("over chunkItems", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.testWriter(ctx, 3, 3, 4, 2)
	})

	t.Run("under maxSemSize", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.testWriter(ctx, 3, 3, 2, 1)
	})

	t.Run("over maxSemSize", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.testWriter(ctx, 3, 3, 32, 12)
	})

	t.Run("many", func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		t.testWriter(ctx, 333, 333, 1<<14, 50)
	})
}

func TestBlockFileMultiChunksWriter(t *testing.T) {
	defer goleak.VerifyNone(t)

	suite.Run(t, new(testBlockFileMultiChunksWriter))
}

type testBaseBlockFileChunkWriter struct {
	baseTestBaseBlockFileWriter
}

func (t *testBaseBlockFileChunkWriter) TestWrite() {
	t.Run("compress format", func() {
		input := util.UUID().Bytes()

		origCreateWriter, bufs := t.newCreateWriter()

		compressFormat := "gz"
		var expectedFilename string

		createWriter := func(it base.BlockItemType, workID string) (string, ioWriteFunc, blockFileDoneFileFunc, error) {
			_, w, d, err := origCreateWriter(it, workID)

			return compressFormat, w,
				func(err error, body string) (string, error) {
					_, err = d(err, body)

					expectedFilename = body + ".json." + compressFormat

					return expectedFilename, err
				},
				err
		}

		c := newFuncBaseBlockFileChunkWriter(
			base.BlockItemOperations,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bc, err := f(nil, func(write ioWriteFunc) error {
			_, err := write(input)

			return err
		})
		t.NoError(err)
		t.NotNil(bc)

		t.T().Logf("expected filename: %q", expectedFilename)
		t.Equal(expectedFilename, bc.URI())
		t.Equal(compressFormat, bc.CompressFormat())
		t.True(t.compareChecksum(input, bc.Checksum()))

		bufs(func(s [][]byte) error {
			t.Equal(1, len(s))
			written := s[0]
			t.NotEmpty(written)
			t.Equal(input, written)

			return nil
		})
	})
}

func TestBaseBlockFileChunkWriter(t *testing.T) {
	suite.Run(t, new(testBaseBlockFileChunkWriter))
}
