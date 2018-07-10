package runners

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/bazel"
	"github.com/twitter/scoot/bazel/cas"
	"github.com/twitter/scoot/bazel/execution/bazelapi"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/os/temp"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/snapshot"
	bzsnapshot "github.com/twitter/scoot/snapshot/bazel"
	"github.com/twitter/scoot/snapshot/git/gitfiler"
)

// invoke.go: Invoker runs a Scoot command.

// NewInvoker creates an Invoker that will use the supplied helpers
func NewInvoker(exec execer.Execer, filerMap runner.RunTypeMap, output runner.OutputCreator, tmp *temp.TempDir, stat stats.StatsReceiver) *Invoker {
	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	return &Invoker{exec: exec, filerMap: filerMap, output: output, tmp: tmp, stat: stat}
}

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	exec     execer.Execer
	filerMap runner.RunTypeMap
	output   runner.OutputCreator
	tmp      *temp.TempDir
	stat     stats.StatsReceiver
}

// Run runs cmd
// Run will send updates as the process is running to updateCh.
// The RunStatus'es that come out of updateCh will have an empty RunID
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// updateCh will not close until the run is finished running.
func (inv *Invoker) Run(cmd *runner.Command, id runner.RunID) (abortCh chan<- struct{}, updateCh <-chan runner.RunStatus) {
	abortChFull := make(chan struct{})
	updateChFull := make(chan runner.RunStatus)
	go inv.run(cmd, id, abortChFull, updateChFull)
	return abortChFull, updateChFull
}

// Run runs cmd as run id returning the final ProcessStatus
// Run will send updates the process is running to updateCh.
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// Run will not return until the process is not running.
func (inv *Invoker) run(cmd *runner.Command, id runner.RunID, abortCh chan struct{}, updateCh chan runner.RunStatus) (r runner.RunStatus) {
	log.WithFields(
		log.Fields{
			"runID":  id,
			"tag":    cmd.Tag,
			"jobID":  cmd.JobID,
			"taskID": cmd.TaskID,
		}).Info("*Invoker.run()")
	taskTimer := inv.stat.Latency(stats.WorkerTaskLatency_ms).Time()
	defer func() {
		taskTimer.Stop()
		updateCh <- r
		close(updateCh)
	}()
	start := time.Now()

	// Records various stages of the run
	// TODO opporunity for consolidation with existing timers and metrics as part of larger refactor
	var rts runTimes
	rts.invokeStart = stamp()

	var co snapshot.Checkout
	checkoutCh := make(chan error)

	// Determine RunType from Command SnapshotID
	// This invoker supports RunTypeScoot and RunTypeBazel
	var runType runner.RunType
	if err := bazel.ValidateID(cmd.SnapshotID); err == nil {
		runType = runner.RunTypeBazel
	} else {
		runType = runner.RunTypeScoot
	}
	if _, ok := inv.filerMap[runType]; !ok {
		return runner.FailedStatus(id, fmt.Errorf("Invoker does not have filer for command of RunType: %s", runType),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}

	// Bazel requests - fetch command argv/env from CAS
	// We can also receive a cached result here, in which case we skip invocation
	if runType == runner.RunTypeBazel {
		cachedResult, notExist, err := preProcessBazel(inv.filerMap[runType].Filer, cmd)
		if err != nil {
			failedStatus := runner.FailedStatus(id, fmt.Errorf("Error preprocessing Bazel command: %s", err),
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})

			// If we encounter a cas NotFoundError, set a grpc status message in
			// the failure run status that indicates missing data to client
			if cas.IsNotFoundError(err) {
				log.Info("NotFound error during Bazel preprocess - Setting grpc Status error")
				errStatus, err := getFailedPreconditionStatus(notExist)
				if err != nil {
					log.Errorf("Error generating Failed Precondition status: %s", err)
				} else {
					failedStatus.ActionResult = &bazelapi.ActionResult{GRPCStatus: errStatus}
				}
			}
			return failedStatus
		}
		if cachedResult != nil {
			status := runner.CompleteStatus(id, "", 0,
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			status.ActionResult = cachedResult
			return status
		}
	}

	// if we are checking out a snapshot, start the timer outside of go routine
	var downloadTimer stats.Latency
	if cmd.SnapshotID != "" {
		downloadTimer = inv.stat.Latency(stats.WorkerDownloadLatency_ms).Time()
		inv.stat.Counter(stats.WorkerDownloads).Inc(1)
	}
	rts.checkoutStart = stamp()

	go func() {
		if cmd.SnapshotID == "" {
			// TODO: we don't want this logic to live here, these decisions should be made at a higher level.
			if len(cmd.Argv) > 0 && cmd.Argv[0] != execers.UseSimExecerArg {
				log.WithFields(
					log.Fields{
						"runID":  id,
						"tag":    cmd.Tag,
						"jobID":  cmd.JobID,
						"taskID": cmd.TaskID,
					}).Info("No snapshotID! Using a nop-checkout initialized with tmpDir")
			}
			if tmp, err := inv.tmp.TempDir("invoke_nop_checkout"); err != nil {
				checkoutCh <- err
			} else {
				co = gitfiler.MakeUnmanagedCheckout(string(id), tmp.Dir)
				checkoutCh <- nil
			}
		} else {
			log.WithFields(
				log.Fields{
					"runID":      id,
					"tag":        cmd.Tag,
					"jobID":      cmd.JobID,
					"taskID":     cmd.TaskID,
					"snapshotID": cmd.SnapshotID,
				}).Info("Checking out snapshotID")
			var err error
			co, err = inv.filerMap[runType].Filer.Checkout(cmd.SnapshotID)
			checkoutCh <- err
		}
	}()

	select {
	case <-abortCh:
		go func() {
			if err := <-checkoutCh; err != nil {
				// If there was an error there should be no lingering gitdb locks, so return.
				return
			}
			// If there was no error then we need to release this checkout.
			co.Release()
		}()
		return runner.AbortStatus(id,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	case err := <-checkoutCh:
		// stop the timer
		// note: aborted runs don't stop the timer - the reported download time should remain 0
		// successful and erroring downloads will report time values
		if cmd.SnapshotID != "" {
			downloadTimer.Stop()
		}
		if err != nil {
			failedStatus := runner.FailedStatus(id, err,
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})

			// For Checkout errors from Bazel commands that indicate non-existance, we set a GRPC
			// Status error indicating that the InputRoot data could not be found.
			if runType == runner.RunTypeBazel {
				if _, ok := err.(*bzsnapshot.CheckoutNotExistError); ok {
					log.Info("Checkout for Bazel command returned CheckoutNotExistError - Setting grpc Status error")
					errStatus, err := getCheckoutMissingStatus(cmd.SnapshotID)
					if err != nil {
						log.Errorf("Error generating Failed Precondition status: %s", err)
					} else {
						failedStatus.ActionResult = &bazelapi.ActionResult{GRPCStatus: errStatus}
					}
				}
			}
			return failedStatus
		}
		// Checkout is ok, continue with run and when finished release checkout.
		defer co.Release()
		rts.checkoutEnd = stamp()
	}
	log.WithFields(
		log.Fields{
			"runID":    id,
			"tag":      cmd.Tag,
			"jobID":    cmd.JobID,
			"taskID":   cmd.TaskID,
			"checkout": co.Path(),
		}).Info("Checkout done")

	stdout, err := inv.output.Create(fmt.Sprintf("%s-stdout", id))
	if err != nil {
		return runner.FailedStatus(id, fmt.Errorf("could not create stdout: %v", err),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}
	defer stdout.Close()

	stderr, err := inv.output.Create(fmt.Sprintf("%s-stderr", id))
	if err != nil {
		return runner.FailedStatus(id, fmt.Errorf("could not create stderr: %v", err),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}
	defer stderr.Close()

	stdlog, err := inv.output.Create(fmt.Sprintf("%s-stdlog", id))
	if err != nil {
		return runner.FailedStatus(id, fmt.Errorf("could not create combined stdout/stderr: %v", err),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}
	defer stdlog.Close()

	marker := "###########################################\n###########################################\n"
	format := "%s\n\nDate: %v\nOut: %s\tErr: %s\tOutErr: %s\tCmd:\n%v\n\n%s\n\n\nSCOOT_CMD_LOG\n"
	header := fmt.Sprintf(format, marker, time.Now(), stdout.URI(), stderr.URI(), stdlog.URI(), cmd, marker)
	// TODO Don't add headers for Bazel. Not clear if a switch for this would come at the Worker level
	// (via Invoker -> QueueRunner construction) or Command level (job requestor specifies in e.g. a PlatformProperty)
	if runType != runner.RunTypeBazel {
		stdout.Write([]byte(header))
		stderr.Write([]byte(header))
		stdlog.Write([]byte(header))
	}
	log.WithFields(
		log.Fields{
			"runID":  id,
			"tag":    cmd.Tag,
			"jobID":  cmd.JobID,
			"taskID": cmd.TaskID,
			"stdout": stdout.AsFile(),
			"stderr": stderr.AsFile(),
			"stdlog": stdlog.AsFile(),
		}).Debug("Stdout/Stderr output")

	rts.execStart = stamp() // candidate for availability via Execer
	p, err := inv.exec.Exec(execer.Command{
		Argv:    cmd.Argv,
		EnvVars: cmd.EnvVars,
		Dir:     co.Path(),
		Stdout:  io.MultiWriter(stdout, stdlog),
		Stderr:  io.MultiWriter(stderr, stdlog),
		LogTags: cmd.LogTags,
	})
	if err != nil {
		return runner.FailedStatus(id, fmt.Errorf("could not exec: %v", err),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}

	var timeoutCh <-chan time.Time
	if cmd.Timeout > 0 { // Timeout if applicable
		elapsed := time.Now().Sub(start)
		timeout := time.NewTimer(cmd.Timeout - elapsed)
		timeoutCh = timeout.C
		defer timeout.Stop()
	}

	updateCh <- runner.RunningStatus(id, stdout.URI(), stderr.URI(),
		tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})

	processCh := make(chan execer.ProcessStatus, 1)
	go func() { processCh <- p.Wait() }()
	var st execer.ProcessStatus

	// Wait for process to complete (or cancel if we're told to)
	select {
	case <-abortCh:
		stdout.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask aborted: %v", marker, cmd.String())))
		stderr.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask aborted: %v", marker, cmd.String())))
		p.Abort()
		return runner.AbortStatus(id,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	case <-timeoutCh:
		stdout.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask exceeded timeout %v: %v", marker, cmd.Timeout, cmd.String())))
		stderr.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask exceeded timeout %v: %v", marker, cmd.Timeout, cmd.String())))
		p.Abort()
		log.WithFields(
			log.Fields{
				"cmd":    cmd.String(),
				"tag":    cmd.Tag,
				"jobID":  cmd.JobID,
				"taskID": cmd.TaskID,
			}).Info("Run timedout")
		return runner.TimeoutStatus(id,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	case st = <-processCh:
	}
	log.WithFields(
		log.Fields{
			"runID":    id,
			"tag":      cmd.Tag,
			"jobID":    cmd.JobID,
			"taskID":   cmd.TaskID,
			"status":   st,
			"checkout": co.Path(),
		}).Info("Run done")

	switch st.State {
	case execer.COMPLETE:
		rts.execEnd = stamp()
		rts.outputStart = stamp()
		if runType == runner.RunTypeScoot {
			tmp, err := inv.tmp.TempDir("invoke")
			if err != nil {
				return runner.FailedStatus(id, fmt.Errorf("error staging ingestion dir: %v", err),
					tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			}
			uploadTimer := inv.stat.Latency(stats.WorkerUploadLatency_ms).Time()
			inv.stat.Counter(stats.WorkerUploads).Inc(1)
			defer func() {
				os.RemoveAll(tmp.Dir)
				uploadTimer.Stop()
			}()

			stdoutName := "STDOUT"
			stderrName := "STDERR"
			stdlogName := "STDLOG"
			if err = stageLogFiles(tmp.Dir, stdoutName, stderrName, stdlogName, stdout, stderr, stdlog); err != nil {
				return runner.FailedStatus(id, err,
					tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			}

			ingestCh := make(chan interface{})
			go func() {
				snapshotID, err := inv.filerMap[runType].Filer.Ingest(tmp.Dir)
				if err != nil {
					ingestCh <- err
				} else {
					ingestCh <- snapshotID
				}
			}()

			// Meaningful to support Abort after execer has completed?
			var snapshotID string
			select {
			case <-abortCh:
				return runner.AbortStatus(id, tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			case res := <-ingestCh:
				switch res.(type) {
				case error:
					return runner.FailedStatus(id, fmt.Errorf("error ingesting results: %v", res),
						tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
				}
				snapshotID = res.(string)
			}
			rts.outputEnd = stamp()
			rts.invokeEnd = stamp()

			// Note: only modifying stdout/stderr refs when we're actively working with snapshotID.
			status := runner.CompleteStatus(id, snapshotID, st.ExitCode,
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			if cmd.SnapshotID != "" {
				status.StdoutRef = snapshotID + "/" + stdoutName
				status.StderrRef = snapshotID + "/" + stderrName
			}
			return status
		} else if runType == runner.RunTypeBazel {
			// Process Bazel uploads of std* output and other data to CAS
			ingestCh := make(chan interface{})
			go func() {
				actionResult, err := postProcessBazel(inv.filerMap[runType].Filer, cmd, co.Path(), stdout, stderr, st, rts)
				if err != nil {
					ingestCh <- err
				} else {
					ingestCh <- actionResult
				}
			}()

			// Meaningful to support Abort after execer has completed?
			var actionResult *bazelapi.ActionResult
			select {
			case <-abortCh:
				return runner.AbortStatus(id, tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			case res := <-ingestCh:
				switch res.(type) {
				case error:
					return runner.FailedStatus(id, fmt.Errorf("Error postprocessing Bazel command: %s", res),
						tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
				}
				actionResult = res.(*bazelapi.ActionResult)
			}

			status := runner.CompleteStatus(id, "", st.ExitCode,
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			status.ActionResult = actionResult
			return status
		} else {
			// should never have an unknown RunType here
			return runner.FailedStatus(id, fmt.Errorf("Can't process Completed status for RunType %s", runType),
				tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		}
	case execer.FAILED:
		return runner.FailedStatus(id, fmt.Errorf("error execing: %v", st.Error),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	default:
		return runner.FailedStatus(id, fmt.Errorf("unexpected exec state: %v", st),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}
}

// stage output files to single directory for snapshot ingestion
func stageLogFiles(tmpDir, stdoutName, stderrName, stdlogName string, stdout, stderr, stdlog runner.Output) error {
	outPath := stdout.AsFile()
	errPath := stderr.AsFile()
	logPath := stdlog.AsFile()

	if err := copyLogFile(tmpDir, stdoutName, outPath); err != nil {
		return err
	}
	if err := copyLogFile(tmpDir, stderrName, errPath); err != nil {
		return err
	}
	if err := copyLogFile(tmpDir, stdlogName, logPath); err != nil {
		return err
	}
	return nil
}

func copyLogFile(tmpDir, logName, logPath string) error {
	var writer *os.File
	var reader *os.File
	defer writer.Close()
	defer reader.Close()

	if writer, err := os.Create(filepath.Join(tmpDir, logName)); err != nil {
		return fmt.Errorf("error staging ingestion for %s: %v", logName, err)
	} else if reader, err = os.Open(logPath); err != nil {
		return fmt.Errorf("error staging ingestion for %s: %v", logPath, err)
	} else if _, err := io.Copy(writer, reader); err != nil {
		return fmt.Errorf("error staging ingestion for stdout: %v", err)
	}
	return nil
}

// Tracking timestamps for stages of an invoker run.
// Values are only set with non-zero Time when stage has completed successfully.
type runTimes struct {
	invokeStart   time.Time
	invokeEnd     time.Time
	checkoutStart time.Time
	checkoutEnd   time.Time
	execStart     time.Time
	execEnd       time.Time
	outputStart   time.Time
	outputEnd     time.Time
}

// Wrapper around time values to encourage "stamp()" usage so it's harder to lose track of runTimes fields.
// Longer term, we should refactor the Invoker so the checkout/exec/upload phases are
// separated from the implementation logic, which will allow these to be recorded clearly
func stamp() time.Time {
	return time.Now()
}
