// Package repo provides utilities for operating on a git repo.
// Scoot often ends up with multiple git repos. E.g., one reference repo
// and then each checkout is in its own repo.
package repo

import (
	"fmt"
	log "github.com/inconshreveable/log15"
	"os"
	"os/exec"
	"strings"
)

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
	return r.RunCmd(r.Command(args...))
}

// Command creates an exec.Cmd to use to run in this Git Repo
func (r *Repository) Command(args ...string) *exec.Cmd {
	cmd := exec.Command("git", args...)
	cmd.Dir = r.dir
	return cmd
}

// RunCmd runs cmd (that must have been created by Command), returning its output and error
func (r *Repository) RunCmd(cmd *exec.Cmd) (string, error) {
	log.Debug("repo.Repository.Run", cmd.Args[1:])
	data, err := cmd.Output()
	log.Debug("repo.Repository.Run complete", err)
	if err != nil && err.(*exec.ExitError) != nil {
		log.Debug("repo.Repository.Run error:", string(err.(*exec.ExitError).Stderr))
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
func (r *Repository) RunCmdSha(cmd *exec.Cmd) (string, error) {
	out, err := r.RunCmd(cmd)
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
	log.Debug("git.NewRepository:", dir, topLevel)
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
