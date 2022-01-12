package hint

const HintedJSONTag = "_hint"

func (ht BaseHinter) IsValid([]byte) error {
	return ht.HT.IsValid(nil)
}

type HintedJSONHead struct {
	H Hint `json:"_hint"`
}

func NewHintedJSONHead(h Hint) HintedJSONHead {
	return HintedJSONHead{H: h}
}
