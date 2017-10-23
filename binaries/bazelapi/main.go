package main

import (
	"flag"

	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/log/hooks"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/scootapi"
)

// CLI tool for making client requests to the Bazel gRPC remote execution API
// Currently supports making a hardcoded Execute request, and prints the response
// The server address and log level are configurable

func main() {
	log.AddHook(hooks.NewContextHook())

	grpcAddr := flag.String("grpc_addr", scootapi.DefaultSched_GRPC, "Address of exec api grpc server")
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

	// Build and send an remoteexecution.ExecuteRequest message

	// Stub to test Execute API
	c := remoteexecution.NewExecutionClient(conn)

	// stub command (this is technically not valid unless we supply the '/bin' dir and '/bin/sleep' ex file)
	cmd := remoteexecution.Command{}
	cmd.Arguments = []string{"/bin/sleep", "10"}
	cmdSha, cmdLen, err := scootproto.GetSha256(&cmd)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to marshal Command for digest: %v\n", err)
	}

	// empty root directory
	dir := remoteexecution.Directory{}
	dirSha, dirLen, err := scootproto.GetSha256(&dir)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to marshal Directory for digest: %v\n", err)
	}

	req := remoteexecution.ExecuteRequest{}
	a := remoteexecution.Action{}
	a.CommandDigest = &remoteexecution.Digest{Hash: cmdSha, SizeBytes: cmdLen}
	a.InputRootDigest = &remoteexecution.Digest{Hash: dirSha, SizeBytes: dirLen}

	req.Action = &a
	req.TotalInputFileCount = 0
	req.TotalInputFileBytes = 0

	// make client request
	res, err := c.Execute(context.Background(), &req)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to make Execute request: %v\n", err)
	}
	log.Infof("Execute response: %s\n", res.String())
}
