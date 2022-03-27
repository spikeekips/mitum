package quictransport

type rawDataType byte

var (
	noneDataType   rawDataType = 0x00
	packetDataType rawDataType = 0x01
	streamDataType rawDataType = 0x02
)

func (dt rawDataType) String() string {
	switch dt {
	case packetDataType:
		return "packet"
	case streamDataType:
		return "stream"
	default:
		return "<unknown>"
	}
}
