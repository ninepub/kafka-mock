package protocol

type MessageBlock struct {
	Offset int64
	Msg    *Message
}

// Messages convenience helper which returns either all the
// messages that are wrapped in this block
func (msb *MessageBlock) Messages() []*MessageBlock {
	if msb.Msg.Set != nil {
		return msb.Msg.Set.Messages
	}
	return []*MessageBlock{msb}
}

func (msb *MessageBlock) Encode(pe PacketEncoder) error {
	pe.PutInt64(msb.Offset)
	pe.Push(&lengthField{})
	err := msb.Msg.Encode(pe)
	if err != nil {
		return err
	}
	pe.Pop()
	return nil
}

func (msb *MessageBlock) Decode(pd PacketDecoder) (err error) {
	if msb.Offset, err = pd.Int64(); err != nil {
		return err
	}

	lengthDecoder := acquireLengthField()
	defer releaseLengthField(lengthDecoder)

	if err = pd.Push(lengthDecoder); err != nil {
		return err
	}

	msb.Msg = new(Message)
	if err = msb.Msg.Decode(pd); err != nil {
		return err
	}

	if err = pd.Pop(); err != nil {
		return err
	}

	return nil
}

type MessageSet struct {
	PartialTrailingMessage bool // whether the set on the wire contained an incomplete trailing MessageBlock
	OverflowMessage        bool // whether the set on the wire contained an overflow message
	Messages               []*MessageBlock
}

func (ms *MessageSet) Encode(pe PacketEncoder) error {
	for i := range ms.Messages {
		err := ms.Messages[i].Encode(pe)
		if err != nil {
			return err
		}
	}
	return nil
}

func (ms *MessageSet) Decode(pd PacketDecoder) (err error) {
	ms.Messages = nil

	for pd.remaining() > 0 {
		magic, err := magicValue(pd)
		if err != nil {
			if err == ErrInsufficientData {
				ms.PartialTrailingMessage = true
				return nil
			}
			return err
		}

		if magic > 1 {
			return nil
		}

		msb := new(MessageBlock)
		err = msb.Decode(pd)
		switch err {
		case nil:
			ms.Messages = append(ms.Messages, msb)
		case ErrInsufficientData:
			// As an optimization the server is allowed to return a partial message at the
			// end of the message set. Clients should handle this case. So we just ignore such things.
			if msb.Offset == -1 {
				// This is an overflow message caused by chunked down conversion
				ms.OverflowMessage = true
			} else {
				ms.PartialTrailingMessage = true
			}
			return nil
		default:
			return err
		}
	}

	return nil
}

func (ms *MessageSet) addMessage(msg *Message) {
	block := new(MessageBlock)
	block.Msg = msg
	ms.Messages = append(ms.Messages, block)
}
