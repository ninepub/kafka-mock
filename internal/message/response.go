package message

import (
	"github.com/ninepub/kafka-mock/internal/protocol"
	"github.com/ninepub/kafka-mock/pkg/types"
	"time"
)

var APIVersionsResponse *protocol.APIVersionsResponse

var MetadataResponse *protocol.MetadataResponse

var ProduceResponse *protocol.ProduceResponse

var apiVersions = []protocol.APIVersion{
	{0, 0, 7},
	{1, 0, 10},
	{2, 0, 4},
	{3, 0, 7},
	{4, 0, 1},
	{5, 0, 1},
	{6, 0, 4},
	{7, 0, 1},
	{8, 0, 1},
	{9, 0, 1},
	{10, 0, 1},
	{11, 0, 1},
	{12, 0, 1},
	{13, 0, 1},
	{14, 0, 1},
	{15, 0, 1},
	{16, 0, 1},
	{17, 0, 1},
	{18, 0, 2},
	{19, 0, 1},
	{20, 0, 1},
	{21, 0, 1},
	{22, 0, 1},
	{23, 0, 1},
	{24, 0, 1},
	{25, 0, 1},
	{26, 0, 1},
	{27, 0, 1},
	{28, 0, 1},
	{29, 0, 1},
	{30, 0, 1},
	{31, 0, 1},
	{32, 0, 1},
	{33, 0, 1},
	{34, 0, 1},
	{35, 0, 1},
	{36, 0, 1},
	{37, 0, 1},
	{38, 0, 1},
	{39, 0, 1},
	{40, 0, 1},
	{41, 0, 1},
	{42, 0, 1}}

var partitionMetaData = []*protocol.PartitionMetadata{
	{0, 0, 1, []int32{1}, []int32{1}},
}

var partitionResponse = []*protocol.ProducePartitionResponse{
	{0, 0, 0, time.Now(), 0},
}

func UpdateResponses(params *types.Params) {
	var host = "127.0.0.1"

	if params.Addr != "" {
		host = params.Addr
	}

	// Hardcoded API version response
	APIVersionsResponse = &protocol.APIVersionsResponse{
		APIVersion:   0,
		ErrorCode:    0,
		APIVersions:  apiVersions,
		ThrottleTime: 0,
	}

	// Hardcoded Meta data response
	MetadataResponse = &protocol.MetadataResponse{
		APIVersion: 1,
		Brokers: []*protocol.Broker{
			{1, host, int32(params.Port), nil},
		},
		ControllerID: 1,
		TopicMetadata: []*protocol.TopicMetadata{
			{0, params.Topic, false, partitionMetaData},
			{0, "__consumer_offsets", true, partitionMetaData},
		},
	}

	// Hard coded producer response
	ProduceResponse = &protocol.ProduceResponse{
		APIVersion: 2,
		Responses: []*protocol.ProduceTopicResponse{
			{params.Topic, partitionResponse},
		},
		ThrottleTime: 0,
	}
}
