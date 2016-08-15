package main

import (
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

//TODO(jschiller)
var remote = flag.String("remote", "", "Empty string to run locally. Otherwise: 'DC/ROLE/ENV'")

var logDir = flag.String("logdir", "/tmp/scoot-smoketest", "If empty, write to stdout, else to log file")
var repoDir = flag.String("repo", "$GOPATH/src/github.com/scootdev/scoot", "Path to scoot repo")
var numWorkers = flag.Int("num_workers", 5, "Number of workerserver instances to spin up.")

var cmds = []*exec.Cmd{}

func fatalf(format string, v ...interface{}) {
	fmt.Printf(format+"\n", v...)
	killcmds()
}

func killcmds() {
	for _, cmd := range cmds {
		if cmd.ProcessState == nil && cmd.Process != nil {
			fmt.Printf("Killing: %v\n", cmd.Path)
			p, _ := os.FindProcess(cmd.Process.Pid)
			p.Signal(os.Interrupt)
		}
	}
}

func getPort() int {
	l, _ := net.Listen("tcp", "localhost:0")
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func run(wait bool, name string, args ...string) *exec.Cmd {
	bin := filepath.Join(os.Getenv("GOPATH"), "bin", name)
	cmd := exec.Command(bin, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmds = append(cmds, cmd)
	if err := cmd.Start(); err != nil {
		fatalf("Error starting %v,%v: %v", bin, args, err)
	}
	if wait {
		go func() {
			if err := cmd.Wait(); err != nil {
				fatalf("Exiting %v,%v: %v", bin, args, err)
			}
		}()
	}
	return cmd
}

func main() {
	if *numWorkers < 5 {
		fatalf("Must have at least 5 workers (see scheduler.go:getNumNodes), currently: %vv", *numWorkers)
	}
	if os.Getenv("GOPATH") == "" {
		fatalf("GOPATH env var must be set")
	}
	if err := os.MkdirAll(*logDir, 0777); err != nil {
		fatalf("Could not create log dir: %v", *logDir)
	}

	cmd := exec.Command("make", "install")
	cmd.Dir = os.ExpandEnv(*repoDir)
	out, err := cmd.CombinedOutput()
	if err != nil {
		fatalf(string(out), err)
	}

	if *logDir != "" {
		logpath := path.Join(*logDir, "smoketest"+strconv.Itoa(os.Getpid()))
		out, err := os.Create(logpath)
		if err != nil {
			fatalf("Could not init logfile: %v", logpath)
		}
		fmt.Println("Redirecting to log: ", logpath)
		os.Stdout = out
		os.Stderr = out
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		var s os.Signal
		s = <-sigchan
		fmt.Printf("signal %s received, shutting down", s)
		killcmds()
		os.Exit(1)
	}()

	for idx := 0; idx < *numWorkers; idx++ {
		port := strconv.Itoa(getPort())
		run(true, "workerserver", "-thrift_port", port)
		fmt.Println("Started worker on port: ", port)
	}
	run(true, "scheduler") //assumes ClusterLocalConfig is the default.
	fmt.Println("Started scheduler on default port")
	testCmd := run(false, "scootapi", "run_smoke_test")
	fmt.Println("Started smoke test.")

	result := testCmd.Wait()
	killcmds()

	fmt.Println("Done")
	if result != nil {
		os.Exit(1)
	} else {
		os.Exit(0)
	}
}
