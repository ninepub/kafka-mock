// Encode and Decode helpers for Kafka implementation
package server

import (
	"github.com/ninepub/kafka-mock/internal/protocol"

	"encoding/json"
	"fmt"
	"io/ioutil"
)

func decodeHeader(b []byte) (*protocol.RequestHeader, *protocol.ByteDecoder, error) {
	d := protocol.NewDecoder(b)
	header := &protocol.RequestHeader{}
	if err := header.Decode(d); err != nil {
		fmt.Println("msg", "failed to decode header", "err", err)
		return nil, nil, err
	}
	return header, d, nil
}

func decodeProduceRequest(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.ProduceRequest, error) {
	req := &protocol.ProduceRequest{}
	if err := req.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode request", "err", err)
		return nil, err
	}
	return req, nil
}

func decodeMetadataRequest(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.MetadataRequest, error) {
	req := &protocol.MetadataRequest{}
	if err := req.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode request", "err", err)
		return nil, err
	}
	return req, nil
}

func decodeApiVersionRequest(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.APIVersionsRequest, error) {
	req := &protocol.APIVersionsRequest{}
	if err := req.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode request", "err", err)
		return nil, err
	}
	return req, nil
}

func decodeProduceResponse(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.ProduceResponse, error) {
	res := &protocol.ProduceResponse{}
	if err := res.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode response", "err", err)
		return nil, err
	}
	return res, nil
}

func decodeMetadataResponse(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.MetadataResponse, error) {
	res := &protocol.MetadataResponse{}
	if err := res.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode response", "err", err)
		return nil, err
	}
	return res, nil
}

func decodeApiVersionResponse(d *protocol.ByteDecoder, header *protocol.RequestHeader) (*protocol.APIVersionsResponse, error) {
	_, err := d.Int16()

	if err != nil {
		return nil, err
	}
	res := &protocol.APIVersionsResponse{}
	if err := res.Decode(d, header.APIVersion); err != nil {
		fmt.Println("msg", "failed to decode response", "err", err)
		return nil, err
	}
	return res, nil
}

func getRecords(data *protocol.ProduceRequest) map[string][]byte {
	records := make(map[string][]byte)
	for topic, topicData := range data.Records {
		fmt.Println("Topic :", topic)
		for r, topicBatch := range topicData {
			fmt.Println("Topic record number  :", r+1)
			if topicBatch.RecordsType == 1 {
				fmt.Println("Legacy record type....")
				for _, msg := range topicBatch.MsgSet.Messages {
					records[string(msg.Msg.Key)] = msg.Msg.Value
				}
			} else {
				fmt.Println("Default record type....")
				for _, msg := range topicBatch.RecordBatch.Records {
					records[string(msg.Key)] = msg.Value
				}
			}
		}
	}
	return records
}

func WriteToJson(data interface{}, path string) error {
	file, err := json.MarshalIndent(data, "", " ")
	if err != nil {
		return err
	}
	err = ioutil.WriteFile(path, file, 0644)
	if err != nil {
		return err
	}
	return nil
}
