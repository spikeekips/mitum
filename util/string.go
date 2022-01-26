package util

type Stringer func() string

func (s Stringer) String() string {
	return s()
}
