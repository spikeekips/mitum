package util

import (
	"os"
	"path/filepath"
)

func CleanDirectory(root string, filter func(path string) bool) error {
	e := StringError("clean directory")

	switch fi, err := os.Stat(root); {
	case err == nil:
		if !fi.IsDir() {
			return e.Errorf("not directory")
		}
	case os.IsNotExist(err):
		return nil
	default:
		return e.Wrap(err)
	}

	subs, err := os.ReadDir(root)
	if err != nil {
		return e.Wrap(err)
	}

	for i := range subs {
		n := subs[i].Name()

		if !filter(n) {
			continue
		}

		if err := os.RemoveAll(filepath.Join(root, n)); err != nil {
			return e.Wrap(err)
		}
	}

	return nil
}
