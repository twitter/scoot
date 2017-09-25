package cli

// package cli implements a cli for the SnapshotDB
// This is our first Scoot CLI that works very well with Cobra and also flags that
// main wants to set. How?
//
// First, main.go (either open-source, closed-source, or some future one) runs.
//
// main.go defines its own impl of DBInjector and constructs it; call it DBIImpl.
//
// main.go calls MakeDBCLI with DBIImpl
//
// [not yet needed/implemented] MakeDBCLI calls DBIImpl.RegisterFlags, which registers
//   the flags that main needs. These may be related to closed-source impls; e.g., which
//   backend server to use.

// MakeDBCLI creates the cobra commands and subcommands
//   (for each cobra command, there will be a dbCommand)
//   creatings the cobra command involves:
//     calling dbCommand.register(), which will register the common functionality flags
//     creating the cobra command with RunE as a wrapper function that will call the DBInjector()
//
// MakeDBCLI returns the root *cobra.Command
//
// main.go calls cmd.Execute()
//
// cobra will parse the command-line flags
//
// cobra will call cmd's RunE, which includes the wrapper defined in MakeDBCLI
//
// the wrapper will call DBInjector.Inject(), which will be DBIImpl.Inject()
//
// DBIImpl.Inject() will construct a SnapshotDB
// the wrapper will call dbCommand.run() with the db, the cobra command (which holds the
//   registered flags) and the additional command-line args
//
// dbCommand.run() does the work of calling a function on the SnapshotDB
import (
	"crypto/sha1"
	"fmt"
	"os"
	"path"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/snapshot"
	"github.com/twitter/scoot/snapshot/bundlestore"
	"github.com/twitter/scoot/snapshot/git/gitdb"
	"github.com/twitter/scoot/snapshot/git/repo"
)

type DBInjector interface {
	// TODO(dbentley): we probably want a way to register flags
	RegisterFlags(cmd *cobra.Command)
	Inject() (snapshot.DB, error)
}

func MakeDBCLI(injector DBInjector) *cobra.Command {
	rootCobraCmd := &cobra.Command{
		Use:   "scoot-snapshot-db",
		Short: "scoot snapshot db CLI",
	}

	injector.RegisterFlags(rootCobraCmd)

	add := func(subCmd dbCommand, parentCobraCmd *cobra.Command) {
		cmd := subCmd.register()
		cmd.RunE = func(innerCmd *cobra.Command, args []string) error {
			db, err := injector.Inject()
			if err != nil {
				return err
			}
			return subCmd.run(db, innerCmd, args)
		}
		parentCobraCmd.AddCommand(cmd)
	}

	createCobraCmd := &cobra.Command{
		Use:   "create",
		Short: "create a snapshot",
	}
	rootCobraCmd.AddCommand(createCobraCmd)

	add(&ingestGitWorkingDirCommand{}, createCobraCmd)
	add(&ingestGitCommitCommand{}, createCobraCmd)
	add(&ingestDirCommand{}, createCobraCmd)
	add(&createGitBundleCommand{}, createCobraCmd)

	readCobraCmd := &cobra.Command{
		Use:   "read",
		Short: "read data from a snapshot",
	}
	rootCobraCmd.AddCommand(readCobraCmd)

	add(&catCommand{}, readCobraCmd)

	exportCobraCmd := &cobra.Command{
		Use:   "export",
		Short: "export a snapshot",
	}
	rootCobraCmd.AddCommand(exportCobraCmd)

	add(&exportGitCommitCommand{}, exportCobraCmd)

	return rootCobraCmd
}

type dbCommand interface {
	register() *cobra.Command
	run(db snapshot.DB, cmd *cobra.Command, args []string) error
}

type ingestGitWorkingDirCommand struct{}

func (c *ingestGitWorkingDirCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest_git_working_dir",
		Short: "ingests HEAD plus the git working dir (into the repo in cwd)",
	}
	return cmd
}

func (c *ingestGitWorkingDirCommand) run(db snapshot.DB, _ *cobra.Command, _ []string) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: wd")
	}

	ingestRepo, err := repo.NewRepository(wd)
	if err != nil {
		return fmt.Errorf("not a valid repo dir: %v, %v", wd, err)
	}

	id, err := db.IngestGitWorkingDir(ingestRepo)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

type ingestGitCommitCommand struct {
	commit string
}

func (c *ingestGitCommitCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest_git_commit",
		Short: "ingests a git commit into the repo in cwd and uploads it",
	}
	cmd.Flags().StringVar(&c.commit, "commit", "", "commit to ingest")
	return cmd
}

func (c *ingestGitCommitCommand) run(db snapshot.DB, _ *cobra.Command, _ []string) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: wd")
	}

	ingestRepo, err := repo.NewRepository(wd)
	if err != nil {
		return fmt.Errorf("not a valid repo dir: %v, %v", wd, err)
	}

	id, err := db.IngestGitCommit(ingestRepo, c.commit)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

// Subcommand for creating and uploading git bundles.
// This is a workaround for creating arbitrary git bundles and keeping them in a Bundlestore.
// We need this for now because generic bundles do not fit well with the existing
// Snapshot, ID, Stream schemas.
// Example usage: scoot-snapshot-db create publish_git_bundle \
//	--basis="<rev>" --ref="master" --ttl="336h" --bundlestore_url="http://localhost:9094/bundle"
// Stdout: "http://localhost:9094/bundle/bs-<rev>-master.bundle"
type createGitBundleCommand struct {
	basis      string        // Git bundle basis commit
	ref        string        // Git ref to bundle (will contain `rev-list basis..ref`)
	ttld       time.Duration // TTL of uploaded bundle in Bundlestore
	outputType string        // What data for generated bundle is printed to stdout
}

const outputTypeLocation = "location"      // Print the location of the uploaded bundle (e.g. the URL)
const outputTypeSnapshotID = "snapshot-id" // Print the snapshot ID of the uploaded bundle

var outputTypes = []string{outputTypeLocation, outputTypeSnapshotID}

func (c *createGitBundleCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish_git_bundle",
		Short: "Creates and uploads a git bundle file as specified by basis/ref for the cwd repo",
	}
	// See https://git-scm.com/docs/git-bundle for basis, ref usage
	cmd.Flags().StringVar(&c.basis, "basis", "", "Basis for bundle")
	cmd.Flags().StringVar(&c.ref, "ref", "master", "Reference to be packaged")
	cmd.Flags().DurationVar(&c.ttld, "ttl", bundlestore.DefaultTTL, "Stored bundle TTL (duration from now)")
	cmd.Flags().StringVar(&c.outputType, "output", "location", fmt.Sprintf("Output type, one of: %q", outputTypes))
	return cmd
}

// Creates a local bundle file based on basis & reference (see `git help bundle`)
// Bundle name is a sha generated from the rev-list contents of the bundle
// Bundle is uploaded to Bundlestore
// Returns location/URL of uploaded bundle, or SnapshotID of generated bundle
func (c *createGitBundleCommand) run(db snapshot.DB, _ *cobra.Command, _ []string) error {
	if c.basis == "" || c.ref == "" {
		return fmt.Errorf("Create bundle command requires a basis and reference")
	}
	validOutput := false
	for _, o := range outputTypes {
		if o == c.outputType {
			validOutput = true
			break
		}
	}
	if !validOutput {
		return fmt.Errorf("Output type must be one of: %q", outputTypes)
	}

	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: wd")
	}

	ingestRepo, err := repo.NewRepository(wd)
	if err != nil {
		return fmt.Errorf("not a valid repo dir: %v, %v", wd, err)
	}

	gdb, ok := db.(*gitdb.DB)
	if !ok {
		return fmt.Errorf("create bundle requires a gitdb.DB snapshot.DB")
	}

	td, err := temp.TempDirDefault()
	if err != nil {
		return fmt.Errorf("Couldn't create temp dir: %v", err)
	}
	defer func() {
		os.RemoveAll(td.Dir)
	}()

	// Don't use commit sha as bundle name as it could collide with other bundles.
	// Use the rev-list for the given basis/ref to generate a unique sha
	// Use this non-commit sha as bundleKey for a snapshot, but still parse
	// commit sha of provided ref to use as snapshot sha.
	// TODO (dgassaway): this would be better off in a proper library package

	revList := fmt.Sprintf("%s..%s", c.basis, c.ref)
	revData, err := ingestRepo.Run("rev-list", revList)
	if err != nil {
		return fmt.Errorf("Couldn't get rev-list for %s: %v", revList, err)
	}
	log.Infof("Using rev-list for %s:\n%s\n", revList, revData)
	commit, err := ingestRepo.Run("rev-parse", c.ref)
	if err != nil {
		return fmt.Errorf("Couldn't rev-parse ref %s: %v", c.ref, err)
	}
	commit = strings.TrimSpace(commit)

	// Add ref name to the rev-list we are bundling - otherwise we can create collisions
	// where a bundle with the same commit content but different ref name can get the same sha1
	revData += c.ref

	revSha1 := fmt.Sprintf("%x", sha1.Sum([]byte(revData)))
	bundleFilename := path.Join(td.Dir, fmt.Sprintf("bs-%s.bundle", revSha1))

	if _, err := ingestRepo.Run("-c",
		"core.packobjectedgesonlyshallow=0",
		"bundle",
		"create",
		bundleFilename,
		revList); err != nil {
		return err
	}

	ttlP := &bundlestore.TTLValue{TTL: time.Now().Add(c.ttld), TTLKey: bundlestore.DefaultTTLKey}
	location, err := gdb.UploadFile(bundleFilename, ttlP)
	if err != nil {
		return err
	}

	if c.outputType == outputTypeLocation {
		fmt.Println(location)
	} else if c.outputType == outputTypeSnapshotID {
		// Create a bundlestoreSnapshot to be able to return a valid snapshot ID
		bs := gitdb.CreateBundlestoreSnapshot(commit, gitdb.KindGitCommitSnapshot, revSha1, gdb.StreamName())
		fmt.Println(bs.ID())
	}
	return nil
}

type exportGitCommitCommand struct {
	id string
}

func (c *exportGitCommitCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "to_git_commit",
		Short: "exports a GitCommitSnapshot identified by id into the repo in cwd",
	}
	cmd.Flags().StringVar(&c.id, "id", "", "id to export")
	return cmd
}

func (c *exportGitCommitCommand) run(db snapshot.DB, _ *cobra.Command, _ []string) error {
	wd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("cannot get working directory: wd")
	}

	exportRepo, err := repo.NewRepository(wd)
	if err != nil {
		return fmt.Errorf("not a valid repo dir: %v, %v", wd, err)
	}

	commit, err := db.ExportGitCommit(snapshot.ID(c.id), exportRepo)
	if err != nil {
		return err
	}

	fmt.Println(commit)
	return nil
}

type ingestDirCommand struct {
	dir string
}

func (c *ingestDirCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest_dir",
		Short: "ingests a directory into the repo in cwd",
	}
	cmd.Flags().StringVar(&c.dir, "dir", "", "dir to ingest")
	return cmd
}

func (c *ingestDirCommand) run(db snapshot.DB, _ *cobra.Command, _ []string) error {
	id, err := db.IngestDir(c.dir)
	if err != nil {
		return err
	}

	fmt.Println(id)
	return nil
}

type catCommand struct {
	id string
}

func (c *catCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cat",
		Short: "concatenate files from an FSSnapshot to stdout",
	}
	cmd.Flags().StringVar(&c.id, "id", "", "Snapshot ID to read from")
	return cmd
}

func (c *catCommand) run(db snapshot.DB, _ *cobra.Command, filenames []string) error {
	id := snapshot.ID(c.id)
	for _, filename := range filenames {
		data, err := db.ReadFileAll(id, filename)
		if err != nil {
			return err
		}
		fmt.Printf("%s", data)
	}
	return nil
}
