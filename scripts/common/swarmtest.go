package common

import (
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"syscall"
)

type SwarmTest []*exec.Cmd

type SwarmTestOpts struct {
	LogDir     string
	RepoDir    string
	NumWorkers int
	Compile    func() error
	Run        func() error
}

func (s *SwarmTest) InitOptions() (*SwarmTestOpts, error) {
	logDir := flag.String("logdir", "/tmp/scoot-swarmtest", "If empty, write to stdout, else to log file")
	repoDir := flag.String("repo", "$GOPATH/src/github.com/scootdev/scoot", "Path to scoot repo")
	numWorkers := flag.Int("num_workers", 5, "Number of workerserver instances to spin up.")

	flag.Parse()
	if *numWorkers < 5 {
		return nil, errors.New("Need >5 workers (see scheduler.go:getNumNodes)")
	}
	compile := func() error { return s.compile(*repoDir) }
	run := func() error { return s.run(*numWorkers) }
	opts := &SwarmTestOpts{*logDir, *repoDir, *numWorkers, compile, run}
	return opts, nil
}

func (s *SwarmTest) Fatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	s.Kill()
}

func (s *SwarmTest) Kill() {
	for _, cmd := range *s {
		if cmd.ProcessState == nil && cmd.Process != nil {
			fmt.Printf("Killing: %v\n", cmd.Path)
			p, _ := os.FindProcess(cmd.Process.Pid)
			p.Signal(os.Interrupt)
		}
	}
}

func (s *SwarmTest) GetPort() int {
	l, _ := net.Listen("tcp", "localhost:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func (s *SwarmTest) RunCmd(blocking bool, name string, args ...string) error {
	bin := filepath.Join(os.Getenv("GOPATH"), "bin", name)
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	*s = append(*s, cmd)
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("Error starting %v,%v: %v", bin, args, err)
	}
	fmt.Println("Started: ", name, args)

	if !blocking {
		go func() {
			if err := cmd.Wait(); err != nil {
				s.Fatalf("Exiting because cmd ended early %v,%v: %v", bin, args, err)
			}
		}()
		return nil
	} else {
		return cmd.Wait()
	}
}

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

func (s *SwarmTest) compile(repoDir string) error {
	cmd := exec.Command("go", "install", "./binaries/...")
	cmd.Dir = os.ExpandEnv(repoDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Println(string(out))
		return err
	}
	return nil
}

func (s *SwarmTest) run(numWorkers int) error {
	for i := 0; i < numWorkers; i++ {
		port := strconv.Itoa(s.GetPort())
		if err := s.RunCmd(false, "workerserver", "-thrift_port", port); err != nil {
			return err
		}
	}
	if err := s.RunCmd(false, "scheduler", "-sched_config", `{"Cluster": {"Type": "local"}}`); err != nil {
		return err
	}
	return s.RunCmd(true, "scootapi", "run_smoke_test")
}

func (s *SwarmTest) Main(opts *SwarmTestOpts) {
	var err error
	s.StartSignalHandler()
	if err = s.SetupLog(opts.LogDir); err == nil {
		if err = opts.Compile(); err == nil {
			err = opts.Run()
			s.Kill()
		}
	}
	fmt.Println("Done")
	if err != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
