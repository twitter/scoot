package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"flag"
	"fmt"
	"github.com/scootdev/scoot/binaries/scheduler/config"
	"github.com/scootdev/scoot/scootapi/server"
	"log"
)

var configFileName = flag.String("config", "inMemory.json", "Scheduler Configuration.")

func main() {

	config, err := config.Asset(fmt.Sprintf("config/%v", *configFileName))

	if err != nil {
		log.Fatalf("Error Loading Config File: %v, with Error: %v", configFileName, err)
	}

	bag, schema := server.Defaults()
	server.RunServer(bag, schema, []byte(config))
}
