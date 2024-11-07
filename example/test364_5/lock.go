package main

import "sync"

func onceFunc(f func() error) func() error {
	var gerr error

	var once sync.Once

	return func() error {
		once.Do(func() {
			gerr = f()
		})

		return gerr
	}
}
