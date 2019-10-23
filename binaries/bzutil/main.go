package main

// Command line tool to enable testing for Bazel requests in Scoot
// We use this for client-side operations Scoot does not implement,
// and are difficult to reproduce using generic tools like grpc_cli.
// Not part of production deployment
//
// Supports subcommands (--help for usage):
// * remoteexecution.Command protobuf data structure insertion to a CAS
// * longrunning.GetOperation polling of operation/scootjob by name and pretty print of result

import (
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/cenkalti/backoff"
	"github.com/golang/protobuf/proto"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/execution"
	"github.com/twitter/scoot/bazel/remoteexecution"
	"github.com/twitter/scoot/common"
	"github.com/twitter/scoot/common/dialer"
	"github.com/twitter/scoot/common/log/hooks"
	scootproto "github.com/twitter/scoot/common/proto"
	"github.com/twitter/scoot/scootapi"
)

var (
	uploadCmdStr        string = "upload_command"
	uploadActionStr     string = "upload_action"
	execCmdStr          string = "execute"
	batchUploadStr      string = "batch_upload"
	batchDownloadStr    string = "batch_download"
	getOpCmdStr         string = "get_operation"
	cancelOpCmdStr      string = "cancel_operation"
	findMissingOpCmdStr string = "find_missing_blobs"
)
var supportedCommands map[string]bool = map[string]bool{
	uploadCmdStr:        true,
	uploadActionStr:     true,
	batchUploadStr:      true,
	batchDownloadStr:    true,
	execCmdStr:          true,
	getOpCmdStr:         true,
	cancelOpCmdStr:      true,
	findMissingOpCmdStr: true,
}

func main() {
	log.AddHook(hooks.NewContextHook())
	log.SetLevel(log.InfoLevel) // default, can be flag overridden

	// TODO additional commands
	// - upload Directory (tbd)
	// - download outputs, stdout/err

	// Subcommand flag definitions

	// Upload Command
	uploadCommand := flag.NewFlagSet(uploadCmdStr, flag.ExitOnError)
	uploadAddr := uploadCommand.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	uploadEnv := uploadCommand.String("env", "", "comma-separated command environment variables, i.e. \"key1=val1,key2=val2\"")
	uploadOutputFiles := uploadCommand.String("output_files", "", "Output files to ingest as comma-separated list: '/file1,/dir/file2'")
	uploadOutputDirs := uploadCommand.String("output_dirs", "", "Output dirs to ingest as comma-separated list: '/dir'")
	uploadPlatformProps := uploadCommand.String("platform_props", "", "comma-separated command platoform properties, i.e. \"key1=val1,key2=val2\"")
	uploadJson := uploadCommand.Bool("json", false, "Print command digest as JSON to stdout")
	uploadLogLevel := uploadCommand.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Batch Upload
	batchUpload := flag.NewFlagSet(batchUploadStr, flag.ExitOnError)
	batchUploadAddr := batchUpload.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	batchUploadDir := batchUpload.String("dir", "", "dir containing files to upload: '/dir'")
	batchUploadLogLevel := batchUpload.String("log_level", "info", "Log everything at this level and above (error|info|debug), default info")

	// Batch Download
	batchDownload := flag.NewFlagSet(batchDownloadStr, flag.ExitOnError)
	batchDownloadAddr := batchDownload.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	batchDownloadDigests := batchDownload.String("digests", "", "digests identifying the contents to download: '<hash1>/<size1>,<hash2>/<size2>,...")
	batchDownloadDir := batchDownload.String("dir", "./bazel_batch_downloads", "directory to write the download files.  (File names will be the contents sha.)")
	batchDownloadLogLevel := batchDownload.String("log_level", "info", "Log everything at this level and above (error|info|debug), default info")

	// Upload Action
	uploadAction := flag.NewFlagSet(uploadActionStr, flag.ExitOnError)
	actionAddr := uploadAction.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	actionCommandDigest := uploadAction.String("command", "", "Command digest as '<hash>/<size>'")
	actionRootDigest := uploadAction.String("input_root", bazel.DigestToStr(bazel.EmptyDigest()),
		"Input root digest as '<hash>/<size>' (default to empty)")
	actionNoCache := uploadAction.Bool("no_cache", false, "Flag to prevent result caching")
	actionJson := uploadAction.Bool("json", false, "Print action digest as JSON to stdout")
	actionLogLevel := uploadAction.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Execute
	execCommand := flag.NewFlagSet(execCmdStr, flag.ExitOnError)
	execAddr := execCommand.String("grpc_addr", scootapi.DefaultSched_GRPC, "'host:port' of grpc Exec server")
	execActionDigest := execCommand.String("action", "", "Action digest as '<hash>/<size>'")
	execSkipCache := execCommand.Bool("skip_cache", false, "Skip checking for cached results")
	execJson := execCommand.Bool("json", false, "Print operation as JSON to stdout")
	execLogLevel := execCommand.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Get Operation
	getCommand := flag.NewFlagSet(getOpCmdStr, flag.ExitOnError)
	getAddr := getCommand.String("grpc_addr", scootapi.DefaultSched_GRPC, "'host:port' of grpc Exec server")
	getName := getCommand.String("name", "", "Operation name to query")
	getJson := getCommand.Bool("json", false, "Print operation as JSON to stdout")
	getLogLevel := getCommand.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Cancel Operation
	cancelCommand := flag.NewFlagSet(cancelOpCmdStr, flag.ExitOnError)
	cancelAddr := cancelCommand.String("grpc_addr", scootapi.DefaultSched_GRPC, "'host:port' of grpc Exec server")
	cancelName := cancelCommand.String("name", "", "Operation name to query")
	cancelLogLevel := cancelCommand.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Find Missing Blobs
	findMissingCommand := flag.NewFlagSet(findMissingOpCmdStr, flag.ExitOnError)
	findMissingAddr := findMissingCommand.String("cas_addr", scootapi.DefaultApiBundlestore_GRPC, "'host:port' of grpc CAS server")
	findMissingDigests := findMissingCommand.String("digests", "", "Digests to find as ','-separated '<hash>/<size>' list")
	findMissingJson := findMissingCommand.Bool("json", false, "Print missing digests as JSON to stdout")
	findMissingLogLevel := findMissingCommand.String("log_level", "", "Log everything at this level and above (error|info|debug)")

	// Parse input flags
	if len(os.Args) < 2 {
		printSupported()
		os.Exit(1)
	}
	switch os.Args[1] {
	case uploadCmdStr:
		uploadCommand.Parse(os.Args[2:])
	case batchUploadStr:
		batchUpload.Parse(os.Args[2:])
	case uploadActionStr:
		uploadAction.Parse(os.Args[2:])
	case batchDownloadStr:
		batchDownload.Parse(os.Args[2:])
	case execCmdStr:
		execCommand.Parse(os.Args[2:])
	case getOpCmdStr:
		getCommand.Parse(os.Args[2:])
	case cancelOpCmdStr:
		cancelCommand.Parse(os.Args[2:])
	case findMissingOpCmdStr:
		findMissingCommand.Parse(os.Args[2:])
	default:
		printSupported()
		os.Exit(1)
	}

	// Run parsed subcommand
	if uploadCommand.Parsed() {
		uploadArgv := uploadCommand.Args()
		if len(uploadArgv) == 0 {
			log.Fatalf("Argv required for %s - will interpret all non-flag arguments as Argv", uploadCmdStr)
		}
		parseAndSetLevel(*uploadLogLevel)
		uploadBzCommand(uploadArgv, *uploadAddr, *uploadEnv, *uploadOutputFiles, *uploadOutputDirs, *uploadPlatformProps, *uploadJson)
	} else if uploadAction.Parsed() {
		if *actionCommandDigest == "" || *actionRootDigest == "" {
			log.Fatalf("command and input_root required for %s", execCmdStr)
		}
		parseAndSetLevel(*actionLogLevel)
		uploadBzAction(*actionAddr, *actionCommandDigest, *actionRootDigest, *actionNoCache, *actionJson)
	} else if batchUpload.Parsed() {
		parseAndSetLevel(*batchUploadLogLevel)
		if *batchUploadDir == "" {
			log.Fatalf("Must supply an upload dir")
		}
		e := batchUploadFiles(*batchUploadAddr, *batchUploadDir)
		if e != nil {
			log.Fatalf("batch upload returned %s", e.Error())
		}
	} else if batchDownload.Parsed() {
		parseAndSetLevel(*batchDownloadLogLevel)
		e := downloadTheBatch(*batchDownloadAddr, *batchDownloadDigests, *batchDownloadDir)
		if e != nil {
			log.Fatal(e)
		}
		log.Infof("contents downloaded to: %s", *batchDownloadDir)
	} else if execCommand.Parsed() {
		if *execActionDigest == "" {
			log.Fatalf("action digest required for %s", execCmdStr)
		}
		parseAndSetLevel(*execLogLevel)
		execute(*execAddr, *execActionDigest, *execSkipCache, *execJson)
	} else if getCommand.Parsed() {
		if *getName == "" {
			log.Fatalf("name required for %s", getOpCmdStr)
		}
		parseAndSetLevel(*getLogLevel)
		getOperation(*getAddr, *getName, *getJson)
	} else if cancelCommand.Parsed() {
		if *cancelName == "" {
			log.Fatalf("name required for %s", cancelOpCmdStr)
		}
		parseAndSetLevel(*cancelLogLevel)
		cancelOperation(*cancelAddr, *cancelName)
	} else if findMissingCommand.Parsed() {
		if *findMissingDigests == "" {
			log.Fatalf("digests requred for %s", findMissingOpCmdStr)
		}
		parseAndSetLevel(*findMissingLogLevel)
		findMissingBlobs(*findMissingAddr, *findMissingDigests, *findMissingJson)
	} else {
		log.Fatal("No expected commands parsed")
	}
}

func parseAndSetLevel(logLevel string) {
	if logLevel == "" {
		return
	}
	level, err := log.ParseLevel(logLevel)
	if err != nil {
		log.Error(err)
		return
	}
	log.SetLevel(level)
}

func uploadBzCommand(cmdArgs []string, casAddr, env, outputFilesStr, outputDirsStr, platformProps string, uploadJson bool) {
	envMap := common.SplitCommaSepToMap(env)
	platMap := common.SplitCommaSepToMap(platformProps)
	log.Infof("Using argv: %q env: %s platform properties: %s", cmdArgs, envMap, platMap)

	// create Command struct from inputs
	cmdEnvVars := []*remoteexecution.Command_EnvironmentVariable{}
	for k, v := range envMap {
		cmdEnvVars = append(cmdEnvVars, &remoteexecution.Command_EnvironmentVariable{Name: k, Value: v})
	}
	cmdPlatformProperties := []*remoteexecution.Platform_Property{}
	for k, v := range platMap {
		cmdPlatformProperties = append(cmdPlatformProperties, &remoteexecution.Platform_Property{Name: k, Value: v})
	}
	cmdPlatform := &remoteexecution.Platform{Properties: cmdPlatformProperties}

	outputFiles := []string{}
	outputDirs := []string{}
	for _, f := range strings.Split(outputFilesStr, ",") {
		if f == "" {
			continue
		}
		outputFiles = append(outputFiles, f)
	}
	for _, d := range strings.Split(outputDirsStr, ",") {
		if d == "" {
			continue
		}
		outputDirs = append(outputDirs, d)
	}

	cmd := &remoteexecution.Command{
		Arguments:            cmdArgs,
		EnvironmentVariables: cmdEnvVars,
		OutputFiles:          outputFiles,
		OutputDirectories:    outputDirs,
		Platform:             cmdPlatform,
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
	r := dialer.NewConstantResolver(casAddr)
	digest := &remoteexecution.Digest{Hash: hash, SizeBytes: size}
	err = cas.MakeCASClient().ByteStreamWrite(r, digest, bytes, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		log.Fatalf("Error writing to CAS: %s", err)
	}

	log.Info("Wrote to CAS successfully")
	log.Info(bazel.DigestToStr(digest))
	if uploadJson {
		b, err := json.Marshal(digest)
		if err != nil {
			log.Fatalf("Error converting digest to JSON: %v", err)
		}
		fmt.Printf("%s\n", b)
	} else {
		fmt.Printf("%s/%d\n", digest.GetHash(), digest.GetSizeBytes())
	}
}

func uploadBzAction(casAddr, commandDigestStr, rootDigestStr string, noCache, actionJson bool) {
	commandDigest, err := bazel.DigestFromString(commandDigestStr)
	if err != nil {
		log.Fatalf("Error converting action to Digest: %s", err)
	}
	rootDigest, err := bazel.DigestFromString(rootDigestStr)
	if err != nil {
		log.Fatalf("Error converting action to Digest: %s", err)
	}

	action := &remoteexecution.Action{
		CommandDigest:   commandDigest,
		InputRootDigest: rootDigest,
		DoNotCache:      noCache,
	}

	// serialize and get hash/size
	bytes, err := proto.Marshal(action)
	if err != nil {
		log.Fatalf("Error serializing action message: %s", err)
	}
	hash, size, err := scootproto.GetSha256(action)
	if err != nil {
		log.Fatalf("Error serializing action message: %s", err)
	}

	// upload action to CAS
	r := dialer.NewConstantResolver(casAddr)
	digest := &remoteexecution.Digest{Hash: hash, SizeBytes: size}
	err = cas.MakeCASClient().ByteStreamWrite(r, digest, bytes, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		log.Fatalf("Error writing to CAS: %s", err)
	}

	log.Info("Wrote to CAS successfully")
	log.Info(bazel.DigestToStr(digest))
	if actionJson {
		b, err := json.Marshal(digest)
		if err != nil {
			log.Fatalf("Error converting digest to JSON: %v", err)
		}
		fmt.Printf("%s\n", b)
	} else {
		fmt.Printf("%s/%d\n", digest.GetHash(), digest.GetSizeBytes())
	}
}

func execute(execAddr, actionDigestStr string, skipCache bool, execJson bool) {
	r := dialer.NewConstantResolver(execAddr)

	actionDigest, err := bazel.DigestFromString(actionDigestStr)
	if err != nil {
		log.Fatalf("Error converting action to Digest: %s", err)
	}

	operation, err := execution.Execute(r, actionDigest, skipCache)
	if err != nil {
		log.Fatalf("Error making Execute request: %s", err)
	}
	log.Info(execution.ExecuteOperationToStr(operation))
	if execJson {
		b, err := json.Marshal(operation)
		if err != nil {
			log.Fatalf("Error converting operation to JSON: %v", err)
		}
		fmt.Printf("%s\n", b)
	} else {
		fmt.Printf("%s\n", operation.GetName())
	}
}

func getOperation(execAddr, opName string, getJson bool) {
	r := dialer.NewConstantResolver(execAddr)
	operation, err := execution.GetOperation(r, opName)
	if err != nil {
		log.Fatalf("Error making GetOperation request: %s", err)
	}
	log.Info(execution.ExecuteOperationToStr(operation))
	// We only use default Marshalling, which leaves most nested fields serialized
	if getJson {
		b, err := json.Marshal(operation)
		if err != nil {
			log.Fatalf("Error converting operation to JSON: %v", err)
		}
		fmt.Printf("%s\n", b)
	} else {
		fmt.Printf("%s\n", operation.GetName())
	}
}

func cancelOperation(execAddr, opName string) {
	r := dialer.NewConstantResolver(execAddr)
	_, err := execution.CancelOperation(r, opName)
	if err != nil {
		log.Fatalf("Error making CancelOperation request: %s", err)
	}
	log.Info("CancelOperation request made successfully")
	// No output
}

func findMissingBlobs(casAddr, digestsStr string, asJson bool) {
	// parse digests from input string
	strs := strings.Split(digestsStr, ",")
	digests := []*remoteexecution.Digest{}
	for _, s := range strs {
		d, err := bazel.DigestFromString(s)
		if err != nil {
			log.Fatalf("Error parsing digest from string %s: %s", s, err)
		}
		digests = append(digests, d)
	}

	r := dialer.NewConstantResolver(casAddr)
	missing, err := cas.FindMissingBlobs(r, digests, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if err != nil {
		log.Fatalf("Error making FindMissingBlobs request: %s", err)
	}

	if asJson {
		b, err := json.Marshal(missing)
		if err != nil {
			log.Fatalf("Error converting missing digests to JSON: %s", err)
		}
		fmt.Printf("%s\n", b)
	} else {
		for _, d := range missing {
			fmt.Printf("%s/%d\n", d.GetHash(), d.GetSizeBytes())
		}
	}
}

// use batch upload, print the list of digests that were uploaded
func batchUploadFiles(casAddr string, uploadDir string) error {
	casClient := cas.MakeCASClient()
	resolver := dialer.NewConstantResolver(casAddr)
	// set up the contents for upload
	contents, e := getContents(uploadDir)
	if e != nil {
		return e
	}

	// request the upload
	digests, e := casClient.BatchUpdateWrite(resolver, contents, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))
	if e != nil {
		return e
	}

	// handle response
	for _, digest := range digests {
		fmt.Printf("%s/%d\n", digest.GetHash(), digest.GetSizeBytes())
	}
	return e
}

// get batch upload contents from either the files in a directory, or test set of 10 blobs
func getContents(dir string) ([]cas.BatchUploadContent, error) {
	contents := make([]cas.BatchUploadContent, 0)
	fileList, e := ioutil.ReadDir(dir)
	if e != nil {
		return nil, e
	}
	for _, f := range fileList {
		if !f.IsDir() {
			data, e := ioutil.ReadFile(fmt.Sprintf("%s/%s", dir, f.Name()))
			if e != nil {
				return nil, fmt.Errorf("error reading %s/%s:%s", dir, f.Name(), e.Error())
			}
			uploadContent := makeBatchUploadContentEntry(data)
			contents = append(contents, uploadContent)
		}
	}
	return contents, nil
}

func makeBatchUploadContentEntry(data []byte) cas.BatchUploadContent {
	sha := sha256.Sum256(data)
	shaStr := fmt.Sprintf("%x", sha)
	digest := &remoteexecution.Digest{
		Hash:      shaStr,
		SizeBytes: int64(len(data)),
	}
	uploadContent := cas.BatchUploadContent{
		Digest: digest,
		Data:   data,
	}
	return uploadContent
}

func downloadTheBatch(casAddr string, digestsStr string, toDir string) error {
	casClient := cas.MakeCASClient()
	resolver := dialer.NewConstantResolver(casAddr)

	// parse the digests into []*Digests
	entries := strings.Split(digestsStr, ",")
	digestsPtr := make([]*remoteexecution.Digest, len(entries))
	for i, entry := range entries {
		d, e := bazel.DigestFromString(entry)
		if e != nil {
			return e
		}
		digestsPtr[i] = d

	}

	// request batch download
	contents, e := casClient.BatchRead(resolver, digestsPtr, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 5))

	if _, err := os.Open(toDir); os.IsNotExist(err) {
		os.Mkdir(toDir, 0777)
	}

	// write any contents returned to toDir
	for sha, content := range contents {
		fname := fmt.Sprintf("%s/%s", toDir, sha)
		f, e := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, 0666)
		if e != nil {
			return fmt.Errorf("error downloading %s to %s: %s", sha, fname, e.Error())
		}
		f.Write(content)
		f.Close()
	}

	// return any error
	return e
}

// Util functions

func printSupported() {
	cmds := make([]string, 0, len(supportedCommands))
	for k := range supportedCommands {
		cmds = append(cmds, k)
	}
	fmt.Printf("Supported commands: %s\n", strings.Join(cmds, ", "))
}
