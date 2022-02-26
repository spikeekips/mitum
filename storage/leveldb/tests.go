//go:build test
// +build test

package leveldbstorage

import (
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

func NewMemWriteStorage() *WriteStorage {
	st, _ := newWriteStorage(leveldbStorage.NewMemStorage(), "")

	return st
}
