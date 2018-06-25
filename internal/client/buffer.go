package client

type buffer struct {
	Bytes  []byte
	Offset int
}

func (b *buffer) Advance(amount int) {
	b.Offset += amount
}
