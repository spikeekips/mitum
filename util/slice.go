package util

func InStringSlice(n string, s []string) bool {
	for _, i := range s {
		if n == i {
			return true
		}
	}

	return false
}
