//go:build test
// +build test

package leveldbstorage

import (
	"github.com/pkg/errors"
	leveldbStorage "github.com/syndtr/goleveldb/leveldb/storage"
)

func NewMemStorage() *Storage {
	st, err := NewStorage(leveldbStorage.NewMemStorage(), nil)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return st
}

func NewFSStorage(f string) *Storage {
	str, err := leveldbStorage.OpenFile(f, false)
	if err != nil {
		panic(errors.WithStack(err))
	}

	st, err := NewStorage(str, nil)
	if err != nil {
		panic(errors.WithStack(err))
	}

	return st
}
