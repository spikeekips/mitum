package hint

type SetHinter interface {
	SetHint(Hint) Hinter
}

type BaseHinter struct {
	HT Hint `json:"_hint"`
}

func NewBaseHinter(ht Hint) BaseHinter {
	return BaseHinter{HT: ht}
}

func (ht BaseHinter) Hint() Hint {
	return ht.HT
}

func (ht BaseHinter) SetHint(n Hint) Hinter {
	ht.HT = n

	return ht
}
