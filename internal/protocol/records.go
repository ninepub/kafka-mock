package protocol

import "fmt"

const (
	unknownRecords = iota
	legacyRecords
	defaultRecords

	magicOffset = 16
	magicLength = 1
)

// Records implements a union type containing either a RecordBatch or a legacy MessageSet.
type Records struct {
	RecordsType int
	MsgSet      *MessageSet
	RecordBatch *RecordBatch
}

// setTypeFromFields sets type of Records depending on which of MsgSet or RecordBatch is not nil.
// The first return value indicates whether both fields are nil (and the type is not set).
// If both fields are not nil, it returns an error.
func (r *Records) setTypeFromFields() (bool, error) {
	if r.MsgSet == nil && r.RecordBatch == nil {
		return true, nil
	}
	if r.MsgSet != nil && r.RecordBatch != nil {
		return false, fmt.Errorf("both MsgSet and RecordBatch are set, but record type is unknown")
	}
	r.RecordsType = defaultRecords
	if r.MsgSet != nil {
		r.RecordsType = legacyRecords
	}
	return false, nil
}

func (r *Records) Encode(pe PacketEncoder) error {
	if r.RecordsType == unknownRecords {
		if empty, err := r.setTypeFromFields(); err != nil || empty {
			return err
		}
	}

	switch r.RecordsType {
	case legacyRecords:
		if r.MsgSet == nil {
			return nil
		}
		return r.MsgSet.Encode(pe)
	case defaultRecords:
		if r.RecordBatch == nil {
			return nil
		}
		return r.RecordBatch.Encode(pe)
	}

	return fmt.Errorf("unknown records type: %v", r.RecordsType)
}

func magicValue(pd PacketDecoder) (int8, error) {
	return pd.PeekInt8(magicOffset)
}

func (r *Records) setTypeFromMagic(pd PacketDecoder) error {
	magic, err := magicValue(pd)
	if err != nil {
		return err
	}

	r.RecordsType = defaultRecords
	if magic < 2 {
		r.RecordsType = legacyRecords
	}

	return nil
}

func (r *Records) Decode(pd PacketDecoder) error {
	if r.RecordsType == unknownRecords {
		if err := r.setTypeFromMagic(pd); err != nil {
			return err
		}
	}

	switch r.RecordsType {
	case legacyRecords:
		r.MsgSet = &MessageSet{}
		return r.MsgSet.Decode(pd)
	case defaultRecords:
		r.RecordBatch = &RecordBatch{}
		return r.RecordBatch.Decode(pd)
	}
	return fmt.Errorf("unknown records type: %v", r.RecordsType)
}
