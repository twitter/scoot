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
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/scootdev/scoot/os/temp"
	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

type DBInjector interface {
	// TODO(dbentley): we probably want a way to register flags
	Inject(storeDir *temp.TempDir) (snapshot.DB, error)
}

func MakeDBCLI(injector DBInjector) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "scoot-snapshot-db",
		Short: "scoot snapshot db CLI",
	}
	add := func(subCmd dbCommand, parentCobraCmd *cobra.Command) {
		cmd := subCmd.register()
		cmd.RunE = func(innerCmd *cobra.Command, args []string) error {
			return subCmd.run(injector, innerCmd, args)
		}
		parentCobraCmd.AddCommand(cmd)
	}

	createCobraCmd := &cobra.Command{
		Use:   "create",
		Short: "create a snapshot",
	}
	rootCmd.AddCommand(createCobraCmd)

	add(&ingestGitCommitCommand{}, createCobraCmd)

	return rootCmd
}

type dbCommand interface {
	register() *cobra.Command
	run(injector DBInjector, cmd *cobra.Command, args []string) error
}

type ingestGitCommitCommand struct {
	commit string
	store  string
}

func (c *ingestGitCommitCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest_git_commit",
		Short: "ingests a git commit into cwd and optionally uploads to a file-backed bundlestore.",
	}
	cmd.Flags().StringVar(&c.commit, "commit", "", "commit to ingest")
	cmd.Flags().StringVar(&c.store, "bundlestore", "", "optional: absolute path for file-backed bundlestore.")
	return cmd
}

func (c *ingestGitCommitCommand) run(injector DBInjector, _ *cobra.Command, _ []string) error {
	var storeDir *temp.TempDir
	if c.store != "" {
		storeDir = &temp.TempDir{Dir: c.store}
	}
	db, err := injector.Inject(storeDir)
	if err != nil {
		return fmt.Errorf("scoot-snapshot-db could not create db: %v", err)
	}

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
