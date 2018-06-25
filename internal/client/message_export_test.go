package client

func (m *Message) Body1() ([]byte, int) {
	return m.body1.Bytes, m.body1.Offset
}
