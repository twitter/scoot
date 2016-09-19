package main

import (
	"github.com/scootdev/scoot/scootapi/server"
)

func main() {
	config := `{
		"Cluster": {
			"Type": "memory",
			"Count": 10
		}
	}`
	bag, schema := server.Defaults()
	server.RunServer(bag, schema, []byte(config))
}
