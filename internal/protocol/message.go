package protocol

import (
	"fmt"
	"time"
)

const (
	//CompressionNone no compression
	CompressionNone CompressionCodec = iota
	//CompressionGZIP compression using GZIP
	CompressionGZIP
	//CompressionSnappy compression using snappy
	CompressionSnappy
	//CompressionLZ4 compression using LZ4
	CompressionLZ4
	//CompressionZSTD compression using ZSTD
	CompressionZSTD

	// The lowest 3 bits contain the compression codec used for the message
	compressionCodecMask int8 = 0x07

	// Bit 3 set for "LogAppend" timestamps
	timestampTypeMask = 0x08

	// CompressionLevelDefault is the constant to use in CompressionLevel
	// to have the default compression level for any codec. The value is picked
	// that we don't use any existing compression levels.
	CompressionLevelDefault = -1000
)

// CompressionCodec represents the various compression codecs recognized by Kafka in messages.
type CompressionCodec int8

func (cc CompressionCodec) String() string {
	return []string{
		"none",
		"gzip",
		"snappy",
		"lz4",
		"zstd",
	}[int(cc)]
}

//Message is a kafka message type
type Message struct {
	Codec            CompressionCodec // codec used to compress the message contents
	CompressionLevel int              // compression level
	LogAppendTime    bool             // the used timestamp is LogAppendTime
	Key              []byte           // the message key, may be nil
	Value            []byte           // the message contents
	Set              *MessageSet      // the message set a message might wrap
	Version          int8             // v1 requires Kafka 0.10
	Timestamp        time.Time        // the timestamp of the message (version 1+ only)

	compressedCache []byte
	compressedSize  int // used for computing the compression ratio metrics
}

func (m *Message) Encode(pe PacketEncoder) error {
	pe.Push(newCRC32Field(crcIEEE))

	pe.PutInt8(m.Version)

	attributes := int8(m.Codec) & compressionCodecMask
	if m.LogAppendTime {
		attributes |= timestampTypeMask
	}
	pe.PutInt8(attributes)

	if m.Version >= 1 {
		if err := (Timestamp{&m.Timestamp}).Encode(pe); err != nil {
			return err
		}
	}

	err := pe.PutBytes(m.Key)
	if err != nil {
		return err
	}

	var payload []byte

	if m.compressedCache != nil {
		payload = m.compressedCache
		m.compressedCache = nil
	} else if m.Value != nil {

		payload, err = compress(m.Codec, m.CompressionLevel, m.Value)
		if err != nil {
			return err
		}
		m.compressedCache = payload
		// Keep in mind the compressed payload size for metric gathering
		m.compressedSize = len(payload)
	}

	if err = pe.PutBytes(payload); err != nil {
		return err
	}

	pe.Pop()
	return nil
}

func (m *Message) Decode(pd PacketDecoder) (err error) {
	crc32Decoder := acquireCrc32Field(crcIEEE)
	defer releaseCrc32Field(crc32Decoder)

	err = pd.Push(crc32Decoder)
	if err != nil {
		return err
	}

	m.Version, err = pd.Int8()
	if err != nil {
		return err
	}

	if m.Version > 1 {
		return PacketDecodingError{fmt.Sprintf("unknown magic byte (%v)", m.Version)}
	}

	attribute, err := pd.Int8()
	if err != nil {
		return err
	}
	m.Codec = CompressionCodec(attribute & compressionCodecMask)
	m.LogAppendTime = attribute&timestampTypeMask == timestampTypeMask

	if m.Version == 1 {
		if err := (Timestamp{&m.Timestamp}).Decode(pd); err != nil {
			return err
		}
	}

	m.Key, err = pd.Bytes()
	if err != nil {
		return err
	}

	m.Value, err = pd.Bytes()
	if err != nil {
		return err
	}

	// Required for deep equal assertion during tests but might be useful
	// for future metrics about the compression ratio in fetch requests
	m.compressedSize = len(m.Value)

	switch m.Codec {
	case CompressionNone:
		// nothing to do
	default:
		if m.Value == nil {
			break
		}

		m.Value, err = decompress(m.Codec, m.Value)
		if err != nil {
			return err
		}
		if err := m.decodeSet(); err != nil {
			return err
		}
	}

	return pd.Pop()
}

// decodes a message set from a previously encoded bulk-message
func (m *Message) decodeSet() (err error) {
	pd := NewDecoder(m.Value)
	m.Set = &MessageSet{}
	return m.Set.Decode(pd)
}
