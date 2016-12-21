package setup

import (
	"log"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/scootdev/scoot/os/temp"
)

// Cmds runs commands for Cloud Scoot setup.
// Over os/exec, it offers:
// *) (best-effort) clean-up (via Kill())
// *) logging when a process is started
// *) output redirection (TODO)
// *) Utility functions for Fatal logging that also Kill
// *) Kills all commands when any long-lived command finishes (to allow prompt debugging)
// Setup code that might start long-running commands should use
// Command instead of exec.Command, and StartCmd/StartRun instead of cmd.Start/cmd.Run
// Run and Start are convenience methods to make it easier to run stuff
// The main entry points Run() or Start() are convenience
type Cmds struct {
	// commands we are watching (may be unstarted or finished)
	watching []*exec.Cmd
	mu       sync.Mutex
	tmp      *temp.TempDir

	wg     sync.WaitGroup
	killed bool
}

// Create a new Cmds
func NewCmds(tmp *temp.TempDir) *Cmds {
	return &Cmds{
		tmp: tmp,
	}
}

// Create a new Cmds that has a signal handler installed
func NewSignalHandlingCmds(tmp *temp.TempDir) *Cmds {
	r := NewCmds(tmp)
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var sig os.Signal
		sig = <-sigchan
		log.Printf("signal %s received; shutting down", sig)
		r.Kill()
		os.Exit(1)
	}()
	return r
}

// Kill kills all running commands
// NB: note, we can't guarantee this is called before exiting.
// If we really want to be correct, we have to start another process that will do
// the babysitting.
func (c *Cmds) Kill() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we already tried to kill, then don't try to kill again. And don't try to kill
	// in the future.
	if c.killed {
		return
	}
	c.killed = true

	sigchan := make(chan os.Signal, 1)
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		for sig := range sigchan {
			log.Printf("signal %s received; ignoring as we are already killing all", sig)
		}
	}()

	log.Printf("Killing %d cmds", len(c.watching))

	// Wait for all to be done.
	allDoneCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(allDoneCh)
	}()

	// Sending a signal to -PGID sends it to all processes in the proces sgroup.
	pgid := -1 * syscall.Getpgrp()

	if len(c.watching) == 0 {
		return
	}

	// Send SIGINT, then send SIGKILL 5 seconds later if any are still running
	log.Printf("SIGINT: process group %d", pgid)
	if err := syscall.Kill(pgid, syscall.SIGINT); err != nil {
		panic(err)
	}

	// Unlock so that remove can do its job (and signal alldoneCh)
	c.mu.Unlock()
	select {
	case <-allDoneCh:
		log.Printf("All completed")
	case <-time.After(5 * time.Second):
		log.Printf("Still waiting")
	}
	c.mu.Lock()

	if len(c.watching) == 0 {
		return
	}

	log.Printf("SIGKILL: process group%d", pgid)
	if err := syscall.Kill(pgid, syscall.SIGKILL); err != nil {
		panic(err)
	}
}

// Commands creates a Command that is watched (and with appropriate redirection)
func (c *Cmds) Command(path string, arg ...string) *exec.Cmd {
	c.mu.Lock()
	defer c.mu.Unlock()
	cmd := exec.Command(path, arg...)
	// TODO(dbentley): redirect output to files in c.tmp so we don't deluge stdout/stderr
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	c.wg.Add(1)
	c.watching = append(c.watching, cmd)
	return cmd
}

// StartCmd starts a Cmd that was created by Command and expects it to run forever
// If cmd stops, c will call c.Kill() (to allow prompt debugging)
func (c *Cmds) StartCmd(cmd *exec.Cmd) error {
	log.Println("Starting", cmd.Args)
	err := cmd.Start()
	if err == nil {
		go func() {
			cmd.Wait()
			log.Printf("Cmd %v finished", cmd.Path)
			c.remove(cmd)
			c.Kill()
		}()
	}
	return err
}

// RunCmd runs a Cmd that was created by Command
func (c *Cmds) RunCmd(cmd *exec.Cmd) error {
	log.Println("Running", cmd.Args)
	// remove cmd once it's done
	defer c.remove(cmd)
	return cmd.Run()
}

// Start is a convenience method that calls Command and then StartCmd
func (c *Cmds) Start(path string, arg ...string) error {
	cmd := c.Command(path, arg...)
	return c.StartCmd(cmd)
}

// Run is a convenience method that calls Command and then RunCmd
func (c *Cmds) Run(path string, arg ...string) error {
	cmd := c.Command(path, arg...)
	return c.RunCmd(cmd)
}

// stop watching a cmd (because it's done)
func (c *Cmds) remove(cmd *exec.Cmd) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wg.Done()
	for i, other := range c.watching {
		if other == cmd {
			c.watching = append(c.watching[0:i], c.watching[i+1:]...)
		}
	}
}
