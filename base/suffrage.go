package base

type Suffrage interface {
	Exists(Address) bool
	Nodes() []Address
	Len() int
}
