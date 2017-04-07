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

	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
	"github.com/spf13/cobra"
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

	add(&ingestGitCommitCommand{}, createCobraCmd)
	add(&ingestDirCommand{}, createCobraCmd)

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

type ingestGitCommitCommand struct {
	commit string
}

func (c *ingestGitCommitCommand) register() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "ingest_git_commit",
		Short: "ingests a git commit into the repo in cwd",
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
