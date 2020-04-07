package protocol

import (
	"fmt"
	"time"
)

const recordBatchOverhead = 49

type recordsArray []*Record

func (e recordsArray) Encode(pe PacketEncoder) error {
	for _, r := range e {
		if err := r.Encode(pe); err != nil {
			return err
		}
	}
	return nil
}

func (e recordsArray) Decode(pd PacketDecoder) error {
	for i := range e {
		rec := &Record{}
		if err := rec.Decode(pd); err != nil {
			return err
		}
		e[i] = rec
	}
	return nil
}

type RecordBatch struct {
	FirstOffset           int64
	PartitionLeaderEpoch  int32
	Version               int8
	Codec                 CompressionCodec
	CompressionLevel      int
	Control               bool
	LogAppendTime         bool
	LastOffsetDelta       int32
	FirstTimestamp        time.Time
	MaxTimestamp          time.Time
	ProducerID            int64
	ProducerEpoch         int16
	FirstSequence         int32
	Records               []*Record
	PartialTrailingRecord bool
	IsTransactional       bool

	compressedRecords []byte
	recordsLen        int // uncompressed records size
}

func (b *RecordBatch) LastOffset() int64 {
	return b.FirstOffset + int64(b.LastOffsetDelta)
}

func (b *RecordBatch) Encode(pe PacketEncoder) error {
	if b.Version != 2 {
		return PacketEncodingError{fmt.Sprintf("unsupported compression codec (%d)", b.Codec)}
	}
	pe.PutInt64(b.FirstOffset)
	pe.Push(&lengthField{})
	pe.PutInt32(b.PartitionLeaderEpoch)
	pe.PutInt8(b.Version)
	pe.Push(newCRC32Field(crcCastagnoli))
	pe.PutInt16(b.computeAttributes())
	pe.PutInt32(b.LastOffsetDelta)

	if err := (Timestamp{&b.FirstTimestamp}).Encode(pe); err != nil {
		return err
	}

	if err := (Timestamp{&b.MaxTimestamp}).Encode(pe); err != nil {
		return err
	}

	pe.PutInt64(b.ProducerID)
	pe.PutInt16(b.ProducerEpoch)
	pe.PutInt32(b.FirstSequence)

	if err := pe.PutArrayLength(len(b.Records)); err != nil {
		return err
	}

	if b.compressedRecords == nil {
		if err := b.EncodeRecords(pe); err != nil {
			return err
		}
	}
	if err := pe.PutRawBytes(b.compressedRecords); err != nil {
		return err
	}
	pe.Pop()
	pe.Pop()
	return nil
}

func (b *RecordBatch) Decode(pd PacketDecoder) (err error) {
	if b.FirstOffset, err = pd.Int64(); err != nil {
		return err
	}

	batchLen, err := pd.Int32()
	if err != nil {
		return err
	}

	if b.PartitionLeaderEpoch, err = pd.Int32(); err != nil {
		return err
	}

	if b.Version, err = pd.Int8(); err != nil {
		return err
	}

	crc32Decoder := acquireCrc32Field(crcCastagnoli)
	defer releaseCrc32Field(crc32Decoder)

	if err = pd.Push(crc32Decoder); err != nil {
		return err
	}

	attributes, err := pd.Int16()
	if err != nil {
		return err
	}
	b.Codec = CompressionCodec(int8(attributes) & compressionCodecMask)
	b.Control = attributes&controlMask == controlMask
	b.LogAppendTime = attributes&timestampTypeMask == timestampTypeMask
	b.IsTransactional = attributes&isTransactionalMask == isTransactionalMask

	if b.LastOffsetDelta, err = pd.Int32(); err != nil {
		return err
	}

	if err = (Timestamp{&b.FirstTimestamp}).Decode(pd); err != nil {
		return err
	}

	if err = (Timestamp{&b.MaxTimestamp}).Decode(pd); err != nil {
		return err
	}

	if b.ProducerID, err = pd.Int64(); err != nil {
		return err
	}

	if b.ProducerEpoch, err = pd.Int16(); err != nil {
		return err
	}

	if b.FirstSequence, err = pd.Int32(); err != nil {
		return err
	}

	numRecs, err := pd.ArrayLength()
	if err != nil {
		return err
	}
	if numRecs >= 0 {
		b.Records = make([]*Record, numRecs)
	}

	bufSize := int(batchLen) - recordBatchOverhead
	recBuffer, err := pd.RawBytes(bufSize)
	if err != nil {
		if err == ErrInsufficientData {
			b.PartialTrailingRecord = true
			b.Records = nil
			return nil
		}
		return err
	}

	if err = pd.Pop(); err != nil {
		return err
	}

	recBuffer, err = decompress(b.Codec, recBuffer)
	if err != nil {
		return err
	}

	b.recordsLen = len(recBuffer)
	err = recordsArray(b.Records).Decode(NewDecoder(recBuffer))
	if err == ErrInsufficientData {
		b.PartialTrailingRecord = true
		b.Records = nil
		return nil
	}
	return nil
}

func (b *RecordBatch) EncodeRecords(pe PacketEncoder) error {
	var raw []byte
	var err error
	if raw, err = Encode(recordsArray(b.Records)); err != nil {
		return err
	}
	b.recordsLen = len(raw)

	b.compressedRecords, err = compress(b.Codec, b.CompressionLevel, raw)
	return err
}

func (b *RecordBatch) computeAttributes() int16 {
	attr := int16(b.Codec) & int16(compressionCodecMask)
	if b.Control {
		attr |= controlMask
	}
	if b.LogAppendTime {
		attr |= timestampTypeMask
	}
	if b.IsTransactional {
		attr |= isTransactionalMask
	}
	return attr
}

func (b *RecordBatch) addRecord(r *Record) {
	b.Records = append(b.Records, r)
}
