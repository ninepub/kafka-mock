# kafka-mock

### CURRENTLY MOCK CAN BE USED ONLY FOR PRODUCERS

### CONSUMER MOCK TBD

TCP server mocking Kafka produce and consumer functions to test messages

Currently only supports produce side mocking. 

The producer records can be retrieved for verification using below code

````
import (
	"fmt"
	"github.com/ninepub/kafka-mock/pkg/server"
)


func verifyKafkaProducer() {
	data := make(chan map[string][]byte)
	params := server.Params{Addr:"", Port:9092, Data:data, Topic: "topic"}
	go server.StartKafka(params)
	for{
            records := <-data
            fmt.Println("Total Records: ", len(records))
            for key, value := range records {
                // Handle the received records here
             }
	}
}
````

The planin docker image can be used to mock and print the byte output of kafka

Docker image can be built locally using below command

````
./build.sh latest
````

To run generated docker image

````
#Running with default config
docker run -it -p 9092:9092 kevin-monteiro/kafka-mock:latest

# Passing custom topic and port
docker run -it -p 9095:9095 kevin-monteiro//kafka-mock:latest --port 9095 --topic test
````