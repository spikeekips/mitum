package util

import (
	"io/ioutil"
	"os"
	"path/filepath"
)

func CleanDirectory(root string, filter func(path string) bool) error {
	e := StringErrorFunc("failed to clean directory")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return e(nil, "not directory")
		}
	case os.IsNotExist(err):
		return nil
	default:
		return e(err, "")
	}

	subs, err := ioutil.ReadDir(root) // FIXME use os.ReadDir
	if err != nil {
		return e(err, "")
	}

	for i := range subs {
		n := subs[i].Name()

		if !filter(n) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(root, n)); err != nil {
			return e(err, "")
		}
	}

	return nil
}
