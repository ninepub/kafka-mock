package protocol

import (
	"encoding/binary"
	"time"
)

const (
	isTransactionalMask   = 0x10
	controlMask           = 0x20
	maximumRecordOverhead = 5*binary.MaxVarintLen32 + binary.MaxVarintLen64 + 1
)

//RecordHeader stores key and value for a record header
type RecordHeader struct {
	Key   []byte
	Value []byte
}

func (h *RecordHeader) Encode(d PacketEncoder) error {
	if err := d.PutVarintBytes(h.Key); err != nil {
		return err
	}
	return d.PutVarintBytes(h.Value)
}

func (h *RecordHeader) Decode(d PacketDecoder) (err error) {
	if h.Key, err = d.VarintBytes(); err != nil {
		return err
	}

	if h.Value, err = d.VarintBytes(); err != nil {
		return err
	}
	return nil
}

//Record is kafka record type
type Record struct {
	Headers []*RecordHeader

	Attributes     int8
	TimestampDelta time.Duration
	OffsetDelta    int64
	Key            []byte
	Value          []byte
	length         varintLengthField
}

func (r *Record) Encode(pe PacketEncoder) error {
	pe.Push(&r.length)
	pe.PutInt8(r.Attributes)
	pe.PutVarint(int64(r.TimestampDelta / time.Millisecond))
	pe.PutVarint(r.OffsetDelta)
	if err := pe.PutVarintBytes(r.Key); err != nil {
		return err
	}
	if err := pe.PutVarintBytes(r.Value); err != nil {
		return err
	}
	pe.PutVarint(int64(len(r.Headers)))

	for _, h := range r.Headers {
		if err := h.Encode(pe); err != nil {
			return err
		}
	}

	pe.Pop()
	return nil
}

func (r *Record) Decode(pd PacketDecoder) (err error) {
	if err = pd.Push(&r.length); err != nil {
		return err
	}

	// Hack fix to get proper decoding going
	pd.Int8()
	// Hack fix to get proper decoding going

	if r.Attributes, err = pd.Int8(); err != nil {
		return err
	}

	timestamp, err := pd.Varint()
	if err != nil {
		return err
	}
	r.TimestampDelta = time.Duration(timestamp) * time.Millisecond

	if r.OffsetDelta, err = pd.Varint(); err != nil {
		return err
	}

	if r.Key, err = pd.VarintBytes(); err != nil {
		return err
	}

	if r.Value, err = pd.VarintBytes(); err != nil {
		return err
	}

	numHeaders, err := pd.Varint()
	if err != nil {
		return err
	}

	if numHeaders >= 0 {
		r.Headers = make([]*RecordHeader, numHeaders)
	}
	for i := int64(0); i < numHeaders; i++ {
		hdr := new(RecordHeader)
		if err := hdr.Decode(pd); err != nil {
			return err
		}
		r.Headers[i] = hdr
	}

	pd.Pop()
	return nil
}
