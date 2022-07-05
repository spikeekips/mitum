package util

import "net/url"

func IsValidURL(u *url.URL) error {
	e := ErrInvalid.Errorf("invalid url")

	switch {
	case len(u.Scheme) < 1:
		return e.Errorf("missing scheme")
	case len(u.Host) < 1:
		return e.Errorf("missing host")
	case len(u.Port()) < 1:
		return e.Errorf("missing port")
	default:
		return nil
	}
}
