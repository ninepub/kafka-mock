package protocol

import (
	"encoding/binary"
	"sync"
)

// LengthField implements the PushEncoder and PushDecoder interfaces for calculating 4-byte lengths.
type lengthField struct {
	startOffset int
	length      int32
}

var lengthFieldPool = sync.Pool{}

func acquireLengthField() *lengthField {
	val := lengthFieldPool.Get()
	if val != nil {
		return val.(*lengthField)
	}
	return &lengthField{}
}

func releaseLengthField(m *lengthField) {
	lengthFieldPool.Put(m)
}

func (l *lengthField) decode(pd PacketDecoder) error {
	var err error
	l.length, err = pd.Int32()
	if err != nil {
		return err
	}
	if l.length > int32(pd.remaining()) {
		return ErrInsufficientData
	}
	return nil
}

func (l *lengthField) SaveOffset(in int) {
	l.startOffset = in
}

func (l *lengthField) ReserveSize() int {
	return 4
}

func (l *lengthField) Fill(curOffset int, buf []byte) error {
	binary.BigEndian.PutUint32(buf[l.startOffset:], uint32(curOffset-l.startOffset-4))
	return nil
}

func (l *lengthField) Check(curOffset int, buf []byte) error {
	if uint32(curOffset-l.startOffset-4) != Encoding.Uint32(buf[l.startOffset:]) {
		// replace this error
		return PacketDecodingError{"invalid length type"}
	}
	return nil
}

type varintLengthField struct {
	startOffset int
	length      int64
}

func (l *varintLengthField) Decode(pd PacketDecoder) error {
	var err error
	l.length, err = pd.Varint()
	return err
}

func (l *varintLengthField) SaveOffset(in int) {
	l.startOffset = in
}

func (l *varintLengthField) adjustLength(currOffset int) int {
	oldFieldSize := l.ReserveSize()
	l.length = int64(currOffset - l.startOffset - oldFieldSize)

	return l.ReserveSize() - oldFieldSize
}

func (l *varintLengthField) ReserveSize() int {
	var tmp [binary.MaxVarintLen64]byte
	return binary.PutVarint(tmp[:], l.length)
}

func (l *varintLengthField) Fill(curOffset int, buf []byte) error {
	binary.PutVarint(buf[l.startOffset:], l.length)
	return nil
}

func (l *varintLengthField) Check(curOffset int, buf []byte) error {
	if int64(curOffset-l.startOffset-l.ReserveSize()) != l.length {
		return PacketDecodingError{"invalid length type"}
	}

	return nil
}
