// All request and response handlers
package server

import (
	"fmt"
	"github.com/ninepub/kafka-mock/internal/message"
	"github.com/ninepub/kafka-mock/internal/protocol"
	"github.com/ninepub/kafka-mock/pkg/types"
	"io"
	"net"
	"time"
)

func handleProduce(conn net.Conn, d *protocol.ByteDecoder, header *protocol.RequestHeader, data chan map[string][]byte) {
	req, err := decodeProduceRequest(d, header)
	if err != nil {
		fmt.Println("msg", "failed to decode request", "err", err)
	}
	records := getRecords(req)

	offset := message.ProduceResponse.Responses[0].PartitionResponses[0].BaseOffset
	message.ProduceResponse.Responses[0].PartitionResponses[0].BaseOffset = offset + 1
	message.ProduceResponse.Responses[0].PartitionResponses[0].LogAppendTime = time.Now()
	handleResponse(conn, message.ProduceResponse, header)

	data <- records
}

func handleMetaData(conn net.Conn, d *protocol.ByteDecoder, header *protocol.RequestHeader) {
	// Handle the Api version request if required here for now we are ignoring the request...

	// Modify the response structure here before sending to client
	handleResponse(conn, message.MetadataResponse, header)
}

func handleApiVersion(conn net.Conn, d *protocol.ByteDecoder, header *protocol.RequestHeader) {
	// Handle the Api version request if required here for now we are ignoring the request...

	// Modify the response structure here before sending to client

	handleResponse(conn, message.APIVersionsResponse, header)
}

func encodeResponse(res interface{}) ([]byte, error) {
	b, err := protocol.Encode(res.(protocol.Encoder))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func handleResponse(conn net.Conn, res protocol.ResponseBody, header *protocol.RequestHeader) error {
	response := &protocol.Response{
		CorrelationID: header.CorrelationID,
		Body:          res,
	}

	b, err := encodeResponse(response)
	if err != nil {
		panic(err)
	}
	_, err = conn.Write(b)
	return err
}

func HandleConnection(conn net.Conn, params *types.Params) {
	remoteAddr := conn.RemoteAddr().String()
	fmt.Println("Client connected from " + remoteAddr)

	defer conn.Close()

	for {
		p := make([]byte, 4)
		_, err := io.ReadFull(conn, p[:])
		if err == io.EOF {
			break
		}
		if err != nil {
			fmt.Println("conn read error: ", err)
			break
		}

		size := protocol.Encoding.Uint32(p)
		if size == 0 {
			break
		}

		b := make([]byte, size+4) //+4 since we're going to copy the size into b
		copy(b, p)

		if _, err = io.ReadFull(conn, b[4:]); err != nil {
			fmt.Println("msg", "failed to read from connection", "err", err)
			panic(err)
		}

		header, d, err := decodeHeader(b)
		if err != nil {
			fmt.Println("msg", "failed to decode header", "err", err)
			panic(err)
		}
		switch header.APIKey {
		case protocol.ProduceKey:
			fmt.Println("Request : Produce Message")
			handleProduce(conn, d, header, params.Data)
		case protocol.MetadataKey:
			fmt.Println("Request : Meta Data")
			handleMetaData(conn, d, header)
		case protocol.APIVersionsKey:
			fmt.Println("Request : Api Version")
			handleApiVersion(conn, d, header)
		default:
			fmt.Println("Unsupported request :", header.APIKey)
		}
	}

	fmt.Println("Client at " + remoteAddr + " disconnected.")
}
