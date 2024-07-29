package main

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	jsonenc "github.com/spikeekips/mitum/util/encoder/json"
	"github.com/stretchr/testify/suite"
)

type baseTestBaseBlockItemWriterNewWriteFunc struct {
	baseTestWorkerMerger
	enc encoder.Encoder
}

func (t *baseTestBaseBlockItemWriterNewWriteFunc) SetupSuite() {
	t.baseTestWorkerMerger.Suite = t.Suite
	t.baseTestWorkerMerger.SetupSuite()

	t.enc = jsonenc.NewEncoder()

	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: base.DummyPublickeyHint, Instance: base.DummyPublickey{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: AccountStateValueHint, Instance: AccountStateValue{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: AccountStateMergeValueHint, Instance: AccountStateMergeValue{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: OpFactHint, Instance: OpFact{}}))
	t.NoError(t.enc.Add(encoder.DecodeDetail{Hint: OpHint, Instance: Op{}}))
}

func (t *baseTestBaseBlockItemWriterNewWriteFunc) newCreateWriter() (
	blockItemWriterCreateWriterFunc,
	func() ([]byte, error),
) {
	w := bytes.NewBuffer(nil)

	return func(base.BlockItemType, string) (ioWriteFunc, func(error) error, error) {
			return w.Write, func(err error) error { return err }, nil
		},
		func() ([]byte, error) {
			defer w.Reset()

			return w.Bytes(), nil
		}
}

func (t *baseTestBaseBlockItemWriterNewWriteFunc) compareChecksum(input []byte, output string) bool {
	write, done := newChecksumWriteFuncs(

		func(b []byte) (int, error) { return len(b), nil },
		sha256.New(),
	)

	_, err := write(input)
	t.NoError(err, "%+v", err)

	return done() == output
}

type testBaseBlockItemWriterNewWriteFunc struct {
	baseTestBaseBlockItemWriterNewWriteFunc
}

func (t *testBaseBlockItemWriterNewWriteFunc) TestWriteHeader() {
	header := "showme"

	t.Run("no write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(header, func(write ioWriteFunc) error {
			return nil
		})
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("write", func() {
		input := util.UUID().Bytes()

		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(header, func(write ioWriteFunc) error {
			_, err := write(input)

			return err
		})
		t.NoError(err)
		t.NotNil(bm)
		t.Equal(base.BlockItemStates, bm.Type())

		var expectedWritten []byte
		{
			hw := bytes.NewBuffer(nil)

			t.NoError(writeBlockItemFileHeader(hw.Write, header))
			_, err = hw.Write(input)
			t.NoError(err)

			expectedWritten = hw.Bytes()
		}

		t.True(t.compareChecksum(expectedWritten, bm.Checksum()))

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)

		t.Equal(expectedWritten, written)
	})

	t.Run("failed to write header", func() {
		input := util.UUID().Bytes()

		createWriter := func(base.BlockItemType, string) (ioWriteFunc, func(error) error, error) {
			return func([]byte) (int, error) {
					return 0, errors.Errorf("findme")
				},
				func(err error) error { return err },
				nil
		}

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(header, func(write ioWriteFunc) error {
			_, err := write(input)

			return err
		})
		t.ErrorContains(err, "findme")
		t.Nil(bm)
	})
}

func (t *testBaseBlockItemWriterNewWriteFunc) TestWrite() {
	t.Run("empty create id", func() {
		createWriter, _ := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		_, err := c("")
		t.ErrorContains(err, "empty create id")
	})

	t.Run("no write", func() {
		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			return nil
		})
		t.NoError(err)
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.Empty(written)
	})

	t.Run("single write", func() {
		input := util.UUID().Bytes()

		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			_, err := write(input)

			return err
		})
		t.NoError(err)
		t.NotNil(bm)
		t.Equal(base.BlockItemStates, bm.Type())

		t.True(t.compareChecksum(input, bm.Checksum()))

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)

		t.Equal(input, written)
	})

	t.Run("multiple write", func() {
		expectedOutput := bytes.NewBuffer(nil)
		inputs := make([][]byte, 33)
		for i := range inputs {
			inputs[i] = util.UUID().Bytes()
			_, err := expectedOutput.Write(inputs[i])
			t.NoError(err)
		}

		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			for i := range inputs {
				if _, err := write(inputs[i]); err != nil {
					return err
				}
			}

			return nil
		})
		t.NoError(err)
		t.NotNil(bm)
		t.Equal(base.BlockItemStates, bm.Type())

		t.True(t.compareChecksum(expectedOutput.Bytes(), bm.Checksum()))

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)

		t.Equal(expectedOutput.Bytes(), written)
	})

	t.Run("write error", func() {
		input := util.UUID().Bytes()

		createWriter, buf := t.newCreateWriter()

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			if _, err := write(input); err != nil {
				return err
			}

			return errors.Errorf("showme")
		})
		t.ErrorContains(err, "showme")
		t.Nil(bm)

		written, err := buf()
		t.NoError(err)
		t.NotEmpty(written)

		t.Equal(input, written)
	})
}

func (t *testBaseBlockItemWriterNewWriteFunc) TestCloseError() {
	t.Run("ok, but error", func() {
		input := util.UUID().Bytes()

		w := bytes.NewBuffer(nil)
		defer w.Reset()

		createWriter := func(base.BlockItemType, string) (ioWriteFunc, func(error) error, error) {
			return w.Write,
				func(err error) error {
					if err != nil {
						return err
					}

					return errors.Errorf("findme")
				},
				nil
		}

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			_, err := write(input)

			return err
		})
		t.ErrorContains(err, "findme")
		t.Nil(bm)

		written := w.Bytes()
		t.NotEmpty(written)

		t.Equal(input, written)
	})

	t.Run("no write, but error", func() {
		w := bytes.NewBuffer(nil)
		defer w.Reset()

		createWriter := func(base.BlockItemType, string) (ioWriteFunc, func(error) error, error) {
			return w.Write,
				func(err error) error {
					if err != nil {
						return err
					}

					return errors.Errorf("findme")
				},
				nil
		}

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			return nil
		})
		t.NoError(err)
		t.Nil(bm)

		t.Empty(w.Bytes())
	})

	t.Run("ok, but done error", func() {
		input := util.UUID().Bytes()

		w := bytes.NewBuffer(nil)
		defer w.Reset()

		createWriter := func(base.BlockItemType, string) (ioWriteFunc, func(error) error, error) {
			return w.Write,
				func(err error) error {
					if err != nil {
						w.Reset()
						t.T().Log("clear written")

						return err
					}

					return nil
				},
				nil
		}

		c := newBaseBlockItemWriterNewWriteFunc(
			base.BlockItemStates,
			createWriter,
		)

		f, err := c(util.UUID().String())
		t.NoError(err)

		bm, err := f(nil, func(write ioWriteFunc) error {
			if _, err := write(input); err != nil {
				return err
			}

			return errors.Errorf("findme")
		})
		t.ErrorContains(err, "findme")
		t.Nil(bm)

		written := w.Bytes()
		t.Empty(written)
	})
}

func TestBaseBlockItemWriterNewWriteFunc(t *testing.T) {
	suite.Run(t, new(testBaseBlockItemWriterNewWriteFunc))
}
