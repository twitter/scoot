package main

import (
	"flag"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/remote/protocol"
	"github.com/twitter/scoot/scootapi"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	grpcAddr := flag.String("grpc_addr", scootapi.DefaultSched_GRPC, "Address of exec api grpc server")
	inputFlag := flag.String("exec_input", "yo", "Input content for Execute RequestPlaceholder")
	logLevelFlag := flag.String("log_level", "info", "Log everything at this level and above (error|info|debug)")
	flag.Parse()

	level, err := log.ParseLevel(*logLevelFlag)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)

	conn, err := grpc.Dial(*grpcAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect to server at %s: %v\n", *grpcAddr, err)
	}
	defer conn.Close()

	// temporary stub to test simple client
	c := protocol.NewExecutionClient(conn)

	r, err := c.Execute(context.Background(), &protocol.RequestPlaceholder{Input: *inputFlag})
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to make Execute request: %v\n", err)
	}
	log.Infof("Execute response: %s\n", r.Output)
}
