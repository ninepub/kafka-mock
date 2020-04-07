// Light Kafka implementation with TCP server
package main

import (
	"github.com/ninepub/kafka-mock/pkg/server"
	"github.com/ninepub/kafka-mock/pkg/types"

	"flag"
	"fmt"
)

var addr = flag.String("addr", "", "The address to listen to; default is \"\" (all interfaces).")
var port = flag.Int("port", 9092, "The port to listen on; default is 9092.")
var topic = flag.String("topic", "mock", "The default mock topic created.")

func main() {
	flag.Parse()
	data := make(chan map[string][]byte)
	server.StartKafka(&types.Params{Addr: *addr, Port: *port, Data: data, Topic: *topic})
	for {
		records := <-data
		// Handle the received records here
		fmt.Println("Total Records: ", len(records))
		for key, value := range records {
			fmt.Println("Key : ", key)
			fmt.Println("Value (length) : ", len(value))
			// fmt.Println("Value (In bytes) : ", value)
		}
	}
}
