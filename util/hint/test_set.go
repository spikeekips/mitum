//go:build test
// +build test

package hint

func (hs *CompatibleSet) MustAdd(hr Hinter) error {
	if err := hs.AddHinter(hr); err != nil {
		panic(err)
	}

	return nil
}
