package main

//go:generate go-bindata -pkg "config" -o ./config/config.go config

import (
	"github.com/scootdev/scoot/binaries/scheduler/config"
	"github.com/scootdev/scoot/scootapi/server"
	"log"
)

func main() {

	configFileName := "config/config.json"
	config, err := config.Asset(configFileName)

	if err != nil {
		log.Fatalf("Error Loading Config File: %v, with Error: %v", configFileName, err)
	}

	bag, schema := server.Defaults()
	server.RunServer(bag, schema, []byte(config))
}
