// All common structs defined here
package types

type Params struct {
	Addr  string
	Port  int
	Topic string
	Data  chan map[string][]byte
}
