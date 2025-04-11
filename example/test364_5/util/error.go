package util

type SError string

func NewSError(s string) *SError {
	e := SError(s)

	return &e
}

func (s *SError) Error() string {
	return string(*s)
}
