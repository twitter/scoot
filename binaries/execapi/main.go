package main

import (
	"crypto/sha256"
	"flag"
	"fmt"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"
	"google.golang.org/grpc"

	"github.com/twitter/scoot/common/log/hooks"
	"github.com/twitter/scoot/scootapi"
)

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

	// Stub to test Execute API
	c := remoteexecution.NewExecutionClient(conn)

	// stub command (this is technically not valid unless we supply the '/bin' dir and '/bin/sleep' ex file)
	cmd := remoteexecution.Command{}
	cmd.Arguments = []string{"/bin/sleep", "10"}
	cmdBytes, err := proto.Marshal(&cmd)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to marshal Command for digest: %v\n", err)
	}
	cmdSha := fmt.Sprintf("%x", sha256.Sum256(cmdBytes))

	// empty root directory
	dir := remoteexecution.Directory{}
	dirBytes, err := proto.Marshal(&dir)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to marshal Directory for digest: %v\n", err)
	}
	dirSha := fmt.Sprintf("%x", sha256.Sum256(dirBytes))

	req := remoteexecution.ExecuteRequest{}
	a := remoteexecution.Action{}
	a.CommandDigest = &remoteexecution.Digest{Hash: cmdSha, SizeBytes: int64(len(cmdBytes))}
	a.InputRootDigest = &remoteexecution.Digest{Hash: dirSha, SizeBytes: int64(len(dirBytes))}

	req.Action = &a
	req.TotalInputFileCount = 0
	req.TotalInputFileBytes = 0

	res, err := c.Execute(context.Background(), &req)
	if err != nil {
		conn.Close()
		log.Fatalf("Failed to make Execute request: %v\n", err)
	}
	log.Infof("Execute response: %s\n", res.String())
}
