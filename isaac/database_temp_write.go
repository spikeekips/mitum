package isaac

import (
	"context"
	"math"
	"sync"

	"github.com/pkg/errors"
	"github.com/spikeekips/mitum/base"
	leveldbstorage "github.com/spikeekips/mitum/storage/leveldb"
	"github.com/spikeekips/mitum/util"
	"github.com/spikeekips/mitum/util/encoder"
	"github.com/spikeekips/mitum/util/hint"
)

type TempWODatabase struct {
	sync.Mutex
	height base.Height
	st     *leveldbstorage.WriteStorage
	encs   *encoder.Encoders
	enc    encoder.Encoder
	m      base.Manifest // NOTE manifest
	sufstt base.State    // NOTE suffrage state
}

func newTempWODatabase(
	height base.Height,
	f string,
	encs *encoder.Encoders,
	enc encoder.Encoder,
) (*TempWODatabase, error) {
	st, err := leveldbstorage.NewWriteStorage(f)
	if err != nil {
		return nil, errors.Wrap(err, "failed new TempWODatabase")
	}

	return &TempWODatabase{height: height, st: st, encs: encs, enc: enc}, nil
}

func (db *TempWODatabase) Close() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) Remove() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.Close(); err != nil {
		return errors.Wrap(err, "failed to close TempWODatabase")
	}

	if err := db.st.Remove(); err != nil {
		return errors.Wrap(err, "failed to remove TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) SetManifest(m base.Manifest) error {
	db.Lock()
	defer db.Unlock()

	if err := db.setManifest(m); err != nil {
		db.m = nil

		return err
	}

	db.m = m

	return nil
}

func (db *TempWODatabase) setManifest(m base.Manifest) error {
	e := util.StringErrorFunc("failed to set manifest")
	if m.Height() != db.height {
		return e(nil, "wrong manifest height")
	}

	b, err := db.marshal(m)
	if err != nil {
		return e(err, "failed to marshal manifest")
	}

	db.st.BatchPut(manifestKey(), b)

	return nil
}

func (db *TempWODatabase) SetStates(sts []base.State) error {
	db.Lock()
	defer db.Unlock()

	if len(sts) < 1 {
		return nil
	}

	e := util.StringErrorFunc("failed to set states in TempWODatabase")

	worker := util.NewErrgroupWorker(context.Background(), math.MaxInt16)
	defer worker.Close()

	var suffragestate base.State
	go func() {
		for i := range sts {
			st := sts[i]

			if _, ok := st.Value().(base.SuffrageStateValue); ok {
				suffragestate = st
			}

			err := worker.NewJob(func(context.Context, uint64) error {
				if st.Height() != db.height {
					return e(nil, "wrong state height")
				}

				b, err := db.marshal(st)
				if err != nil {
					return errors.Wrap(err, "failed to set state")
				}

				db.st.BatchPut(stateKey(st.Hash()), b)

				return nil
			})
			if err != nil {
				break
			}
		}

		worker.Done()
	}()

	if err := worker.Wait(); err != nil {
		return e(err, "")
	}

	db.st.BatchPut(suffrageKey(), suffragestate.Hash().Bytes())
	db.sufstt = suffragestate

	return nil
}

func (db *TempWODatabase) SetOperations(ops []util.Hash) error {
	if len(ops) < 1 {
		return nil
	}

	for i := range ops {
		db.st.BatchPut(operationKey(ops[i]), nil)
	}

	return nil
}

func (db *TempWODatabase) Write() error {
	db.Lock()
	defer db.Unlock()

	if err := db.st.BatchWrite(); err != nil {
		return errors.Wrap(err, "failed to write to TempWODatabase")
	}

	return nil
}

func (db *TempWODatabase) ToRO() (*TempRODatabase, error) {
	db.Lock()
	defer db.Unlock()

	return newTempRODatabaseFromWOStorage(db)
}

func (db *TempWODatabase) marshal(i interface{}) ([]byte, error) {
	b, err := db.enc.Marshal(i)
	if err != nil {
		return nil, err
	}

	return db.encodeWithEncoder(b), nil
}

func (db *TempWODatabase) encodeWithEncoder(b []byte) []byte {
	h := make([]byte, hint.MaxHintLength)
	copy(h, db.enc.Hint().Bytes())

	return util.ConcatBytesSlice(h, b)
}
