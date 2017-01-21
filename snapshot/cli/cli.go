package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/scootdev/scoot/snapshot"
	"github.com/scootdev/scoot/snapshot/git/repo"
)

type DBInjector interface {
	RegisterFlags(rootCmd *cobra.Command)
	Inject() (snapshot.DB, error)
}

func MakeDBCLI(injector DBInjector) *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "scoot-snapshot-db",
		Short: "scoot snapshot db CLI",
	}
	parentCmd := rootCmd
	add := func(subCmd dbCommand) {
		cmd := subCmd.register()
		cmd.RunE = func(innerCmd *cobra.Command, args []string) error {
			db, err := injector.Inject()
			if err != nil {
				return fmt.Errorf("scoot-snapshot-db could not create db: %v", err)
			}
			return subCmd.run(db, innerCmd, args)
		}
		parentCmd.AddCommand(cmd)
	}

	createCmd := &cobra.Command{
		Use:   "create",
		Short: "create a snapshot",
	}
	rootCmd.AddCommand(createCmd)
	parentCmd = createCmd

	add(&ingestGitCommitCommand{})

	return rootCmd
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
		Short: "ingest a git commit",
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
