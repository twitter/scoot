package swarmtest

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/scootdev/scoot/scootapi"
)

// SwarmTest expects a handful of config options and InitOptions() sets the defaults.
// Also maintains a list of Running cmds in order to kill everything on exit.
type SwarmTest struct {
	Running    []*exec.Cmd
	LogDir     string
	RepoDir    string
	NumWorkers int
	Wait       bool
	DoCompile  bool
	Compile    func() error
	Setup      func() (string, error)
	Run        func() error
	NumJobs    int
	Timeout    time.Duration
	mutex      *sync.Mutex
}

// Initializes SwarmTest with defaults or cli flags if provided.
func (s *SwarmTest) InitOptions(defaults map[string]interface{}) error {
	d := map[string]interface{}{
		"logdir":      "",
		"repo":        "$GOPATH/src/github.com/scootdev/scoot",
		"num_workers": 10,
		"num_jobs":    10,
		"timeout":     10 * time.Second,
	}
	for key, val := range defaults {
		d[key] = val
	}
	logDir := flag.String("logdir", d["logdir"].(string), "If empty, write to stdout, else to log file")
	repoDir := flag.String("repo", d["repo"].(string), "Path to scoot repo")
	numWorkers := flag.Int("num_workers", d["num_workers"].(int), "Number of workerserver instances to spin up.")
	numJobs := flag.Int("num_jobs", d["num_jobs"].(int), "Number of Jobs to run")
	timeout := flag.Duration("timeout", d["timeout"].(time.Duration), "Time to wait for jobs to complete")
	wait := flag.Bool("setup_then_wait", false, "if true, don't run tests; just setup and wait")
	doCompile := flag.Bool("compile", false, "Compile twitter scoot binaries prior to running swarm test.")

	flag.Parse()

	s.LogDir = *logDir
	s.RepoDir = *repoDir
	s.NumWorkers = *numWorkers
	s.Wait = *wait
	s.DoCompile = *doCompile
	s.Compile = func() error { return s.compile() }
	s.Setup = func() (string, error) { return s.setup() }
	s.Run = func() error { return s.run() }
	s.NumJobs = *numJobs
	s.Timeout = *timeout
	s.mutex = &sync.Mutex{}
	return nil
}

// Log a message then kill all running commands.
func (s *SwarmTest) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	s.Kill()
}

// Kill each running command by sending an interrupt signal.
// This assumes that we run well behaved commands and don't need to use pgid or force kill.
func (s *SwarmTest) Kill() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, cmd := range s.Running {
		if cmd.ProcessState == nil && cmd.Process != nil {
			fmt.Printf("Killing: %v\n", cmd.Path)
			p, _ := os.FindProcess(cmd.Process.Pid)
			p.Signal(os.Interrupt)
		}
	}
}

// Get any free port.
func (s *SwarmTest) GetPort() int {
	l, _ := net.Listen("tcp", "localhost:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// Run the given command by first evaluating any env vars and redirecting output to global stdout/stderr.
// If blocking, waits for the command to finish and returns any err, otherwise returns nil.
func (s *SwarmTest) RunCmd(blocking bool, name string, args ...string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for i, _ := range args {
		args[i] = os.ExpandEnv(args[i])
	}
	cmd := exec.Command(os.ExpandEnv(name), args...)
	cmd.Dir = os.ExpandEnv(s.RepoDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	s.Running = append(s.Running, cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Error starting %v,%v: %v", name, args, err)
	}
	fmt.Println("Started: ", name, args)

	if !blocking {
		return nil
	} else {
		return cmd.Wait()
	}
}

// If logDir is specified, creates logDir and then redirects global output to a new file in that dir.
// Note: currently names the log 'scoot-{PID}' which may repeat over the course of many swarmtests.
func (s *SwarmTest) SetupLog(logDir string) error {
	if logDir != "" {
		if err := os.MkdirAll(logDir, 0777); err != nil {
			return fmt.Errorf("Could not create log dir: %v", logDir)
		}
		logpath := path.Join(logDir, "swarmtest"+strconv.Itoa(os.Getpid()))
		out, err := os.Create(logpath)
		if err != nil {
			return fmt.Errorf("Could not init logfile: %v", logpath)
		}
		fmt.Println("Redirecting to log: ", logpath)
		os.Stdout = out
		os.Stderr = out
	}
	return nil
}

// Kill all running commands before exiting.
func (s *SwarmTest) StartSignalHandler() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var sig os.Signal
		sig = <-sigchan
		fmt.Printf("signal %s received, shutting down\n", sig)
		s.Kill()
		os.Exit(1)
	}()
}

// Default. Does a golang install to $GOPATH/bin
func (s *SwarmTest) compile() error {
	return s.RunCmd(true, "go", "install", "./binaries/...")
}

// Default. Runs a locally distributed end-to-end test.
// Creates multiple workers, a scheduler, and the scootapi smoketest to drive job requests.
func (s *SwarmTest) setup() (string, error) {
	for i := 0; i < s.NumWorkers; i++ {
		thriftAddr := "localhost:" + strconv.Itoa(s.GetPort())
		httpAddr := "localhost:" + strconv.Itoa(s.GetPort())
		args := []string{"-thrift_addr", thriftAddr, "-http_addr", httpAddr}
		if err := s.RunCmd(false, "$GOPATH/bin/workerserver", args...); err != nil {
			return "", err
		}
	}

	args := []string{"-config", "local.json"}
	if err := s.RunCmd(false, "$GOPATH/bin/scheduler", args...); err != nil {
		return "", err
	}

	return "localhost:9090", nil
}

func (s *SwarmTest) run() error {
	return s.RunCmd(true, "$GOPATH/bin/scootapi", "run_smoke_test", strconv.Itoa(s.NumJobs), s.Timeout.String())
}

// Sets up env and runs the binaries necessary to complete a local smoketest.
func (s *SwarmTest) RunSwarmTest() error {
	var err error
	s.StartSignalHandler()
	if err = s.SetupLog(s.LogDir); err != nil {
		return err
	}

	if s.DoCompile {
		fmt.Println("Compiling")
		if err = s.Compile(); err != nil {
			return err
		}
	}

	fmt.Println("Setting Up")
	addr, err := s.Setup()
	if err != nil {
		return err
	}
	scootapi.SetScootapiAddr(addr)
	log.Println("Scoot is running at", addr)

	if s.Wait {
		// Just wait (and let the user sigint us when done)
		select {}
	} else {
		log.Println("Running")
		return s.Run()
	}
}

// Run the swarmtest and print err, if any, before exiting with a success/fail exit code.
func (s *SwarmTest) Main() {
	err := s.RunSwarmTest()
	s.Kill()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
