package protocol

type ProduceRequest struct {
	TransactionalID *string
	RequiredAcks    int16
	Timeout         int32
	Version         int16 // v1 requires Kafka 0.9, v2 requires Kafka 0.10, v3 requires Kafka 0.11
	Records         map[string]map[int32]Records
}

func (r *ProduceRequest) Decode(d PacketDecoder, version int16) error {
	r.Version = version

	if version >= 3 {
		id, err := d.NullableString()
		if err != nil {
			return err
		}
		r.TransactionalID = id
	}
	requiredAcks, err := d.Int16()
	if err != nil {
		return err
	}
	r.RequiredAcks = requiredAcks
	if r.Timeout, err = d.Int32(); err != nil {
		return err
	}
	topicCount, err := d.ArrayLength()
	if err != nil {
		return err
	}
	if topicCount == 0 {
		return nil
	}

	r.Records = make(map[string]map[int32]Records)
	for i := 0; i < topicCount; i++ {
		topic, err := d.String()
		if err != nil {
			return err
		}
		partitionCount, err := d.ArrayLength()
		if err != nil {
			return err
		}
		r.Records[topic] = make(map[int32]Records)

		for j := 0; j < partitionCount; j++ {
			partition, err := d.Int32()
			if err != nil {
				return err
			}
			size, err := d.Int32()
			if err != nil {
				return err
			}
			recordsDecoder, err := d.Subset(int(size))
			if err != nil {
				return err
			}
			var records Records
			if err := records.Decode(recordsDecoder); err != nil {
				return err
			}
			r.Records[topic][partition] = records
		}
	}

	return nil
}
