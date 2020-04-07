// Core server implementation
package server

import (
	"fmt"
	"github.com/ninepub/kafka-mock/internal/message"
	"github.com/ninepub/kafka-mock/internal/server"
	"github.com/ninepub/kafka-mock/pkg/types"
	"net"
	"strconv"
)

func StartKafka(params *types.Params) {
	// Initialize the response messages
	message.UpdateResponses(params)

	fmt.Println("Starting server...")

	src := params.Addr + ":" + strconv.Itoa(params.Port)
	listener, err := net.Listen("tcp", src)
	if err != nil {
		fmt.Printf("Failed to start the server: %s\n", err)
	}
	fmt.Printf("Listening on %s.\n", src)
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Some connection error: %s\n", err)
		}
		go server.HandleConnection(conn, params)
	}
}
