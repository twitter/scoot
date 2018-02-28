package main

// Command line tool to enable testing for Bazel requests in Scoot
// We use this for client-side operations Scoot does not implement,
// and are difficult to reproduce using generic tools like grpc_cli.
// Not part of production deployment
//
// Supports:
// * remoteexecution.Command protobuf data structure insertion to a CAS

import (
	"flag"
	"fmt"
	"strings"

	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"
	remoteexecution "google.golang.org/genproto/googleapis/devtools/remoteexecution/v1test"

	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/scootapi"
)

func main() {
	log.AddHook(hooks.NewContextHook())

	// TODO include optional directory that gets BzFiler.Ingest'ed, so we have a tool that easily satisfies prereqs

	casAddr := flag.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	argv := flag.String("args", "", "space-separated command arguments, i.e. \"./run arg1 arg2\"")
	env := flag.String("env", "", "comma-separated command environment variables, i.e. \"key1=val1,key2=val2\"")
	flag.Parse()

	cmdArgs := strings.Split(*argv, " ")
	envMap := make(map[string]string)
	for _, pair := range strings.Split(*env, ",") {
		if pair == "" {
			continue
		}
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		envMap[kv[0]] = kv[1]
	}
	log.Infof("Using argv: %q env: %s", cmdArgs, envMap)

	// create Command struct from inputs
	cmdEnvVars := []*remoteexecution.Command_EnvironmentVariable{}
	for k, v := range envMap {
		cmdEnvVars = append(cmdEnvVars, &remoteexecution.Command_EnvironmentVariable{Name: k, Value: v})
	}
	cmd := &remoteexecution.Command{
		Arguments:            cmdArgs,
		EnvironmentVariables: cmdEnvVars,
	}

	// serialize and get hash/size
	bytes, err := proto.Marshal(cmd)
	if err != nil {
		log.Fatalf("Error serializing command message: %s", err)
	}
	hash, size, err := scootproto.GetSha256(cmd)
	if err != nil {
		log.Fatalf("Error serializing command message: %s", err)
	}

	// upload command to CAS
	r := dialer.NewConstantResolver(*casAddr)
	digest := &remoteexecution.Digest{Hash: hash, SizeBytes: size}
	err = cas.ByteStreamWrite(r, digest, bytes)
	if err != nil {
		log.Fatalf("Error writing to CAS: %s", err)
	}

	log.Info("Wrote to CAS successfully")
	fmt.Printf("%s %d\n", hash, size)
}

// TODO support making a GetOperation request to an execution_server addr. (and operation name). Print stuff out.
// Goal is to accurately debug results of GetOperation calls that grpc_cli just dumps as garbage
