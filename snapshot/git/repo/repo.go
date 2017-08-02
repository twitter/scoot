// Package repo provides utilities for operating on a git repo.
// Scoot often ends up with multiple git repos. E.g., one reference repo
// and then each checkout is in its own repo.
package repo

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
)

const gitCommandTimeout = 10 * time.Minute
const gitIndexLock = ".git/index.lock"

// Repository represents a valid Git repository.
type Repository struct {
	dir string
}

// Where r lives on disk
func (r *Repository) Dir() string {
	return r.dir
}

// Run a git command in r
func (r *Repository) Run(args ...string) (string, error) {
	cmd, ctx, cancel := r.Command(args...)
	return r.RunCmd(cmd, ctx, cancel)
}

func (r *Repository) RunExtraEnv(extraEnv []string, args ...string) (string, error) {
	cmd, ctx, cancel := r.Command(args...)
	cmd.Env = append(cmd.Env, extraEnv...)
	return r.RunCmd(cmd, ctx, cancel)
}

// Command creates an exec.Cmd to use to run in this Git Repo
// Forcefully adds a Command Context with a timeout set to gitCommandTimeout
// as a failsafe against hanging git processes. This requires a CancelFunc
// be passed back to the caller - see https://golang.org/pkg/os/exec/#CommandContext
func (r *Repository) Command(args ...string) (*exec.Cmd, context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), gitCommandTimeout)
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = r.dir
	return cmd, ctx, cancel
}

// RunCmd runs cmd (that must have been created by Command), returning its output and error
func (r *Repository) RunCmd(cmd *exec.Cmd, ctx context.Context, cancel context.CancelFunc) (string, error) {
	log.Info("repo.Repository.Run, ", cmd.Args[1:])
	defer cancel()
	data, err := cmd.Output()

	if ctx.Err() == context.DeadlineExceeded {
		r.CleanupKill()
		return string(data), ctx.Err()
	}

	log.Info("repo.Repository.Run complete. Err: ", err)
	if err != nil && err.(*exec.ExitError) != nil {
		log.Info("repo.Repository.Run error: ", string(err.(*exec.ExitError).Stderr))
	}
	return string(data), err
}

// Run a git command that returns a sha.
func (r *Repository) RunSha(args ...string) (string, error) {
	out, err := r.Run(args...)
	if err != nil {
		return out, err
	}
	return validateSha(out)
}

// RunCmdSha runs cmd (that must have been created by Command) expecting a sha
func (r *Repository) RunCmdSha(cmd *exec.Cmd, ctx context.Context, cancel context.CancelFunc) (string, error) {
	out, err := r.RunCmd(cmd, ctx, cancel)
	if err != nil {
		return out, err
	}
	return validateSha(out)
}

func (r *Repository) RunExtraEnvSha(extraEnv []string, args ...string) (string, error) {
	out, err := r.RunExtraEnv(extraEnv, args...)
	if err != nil {
		return out, err
	}
	return validateSha(out)
}

// validateSha trims and validates sha as a git sha, returning the valid sha xor an error
func validateSha(sha string) (string, error) {
	if len(sha) == 40 || len(sha) == 41 && sha[40] == '\n' {
		return sha[0:40], nil
	}
	return "", fmt.Errorf("sha not 40 or 41 (with a \\n) characters: %q", sha)
}

// NewRepo creates a new Repository for path `dir`.
// It checks that `dir` is a valid path.
func NewRepository(dir string) (*Repository, error) {
	// TODO(dbentley): make sure we handle the case that path is in a git repo,
	// but is not the root of a git repo
	r := &Repository{dir}
	// TODO(dbentley): we'd prefer to use features present in git 2.5+, but are stuck on 2.4 for now
	// E.g., --resolve-git-dir or --git-path
	topLevel, err := r.Run("rev-parse", "--show-toplevel")
	if err != nil {
		return nil, err
	}
	topLevel = strings.Replace(topLevel, "\n", "", -1)
	log.Info("git.NewRepository: ", dir, ", top: ", topLevel)
	r.dir = topLevel
	return r, nil
}

// Try to initialize a new git repo in the given directory.
func InitRepo(dir string) (*Repository, error) {
	os.MkdirAll(dir, 0755)
	cmd := exec.Command("git", "init")
	cmd.Dir = dir
	if err := cmd.Run(); err != nil {
		return nil, err
	}
	return NewRepository(dir)
}

// Cleanup actions to take after a git process had to be killed for whatever reason (like timeout).
// This cleanup should not assume any state / be as safe as possible.
func (r *Repository) CleanupKill() {
	lockFile := path.Join(r.dir, gitIndexLock)
	if _, err := os.Stat(lockFile); !os.IsNotExist(err) {
		if err := os.Remove(lockFile); err != nil {
			log.Errorf("Failed to remove git index lock file during cleanup. %s: %v\n", lockFile, err)
		}
	}

	// Don't reuse higher-level public functions for cleanup
	cmd := exec.Command("git", "reset", "--hard", "HEAD")
	cmd.Dir = r.dir
	if err := cmd.Run(); err != nil {
		log.Errorf("Failed to run git reset during cleanup: %v\n", err)
	}

	cmd = exec.Command("git", "clean", "-f", "-f", "-d", "-x")
	cmd.Dir = r.dir
	if err := cmd.Run(); err != nil {
		log.Errorf("Failed to run git clean during cleanup: %v\n", err)
	}
}
