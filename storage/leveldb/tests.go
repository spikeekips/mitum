//go:build test
// +build test

package leveldbstorage

import (
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

func NewMemBatchStorage() *BatchStorage {
	st, _ := newBatchStorage(leveldbStorage.NewMemStorage(), "")

	return st
}

func NewMemReadonlyStorage() *ReadonlyStorage {
	st, _ := newReadonlyStorage(leveldbStorage.NewMemStorage(), "")

	return st
}

func NewMemRWStorage() *RWStorage {
	st, _ := newRWStorage(leveldbStorage.NewMemStorage(), "")

	return st
}
