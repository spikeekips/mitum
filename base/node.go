package base

type Node interface {
	Address() Address
	Publickey() Publickey
}

type LocalNode interface {
	Node
	Privatekey() Privatekey
}
