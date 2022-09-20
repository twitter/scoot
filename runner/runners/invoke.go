package runners

import (
	e "errors"
	"fmt"
	"io"
	"io/ioutil"
	"time"

	uuid "github.com/nu7hatch/gouuid"
	log "github.com/sirupsen/logrus"

	"github.com/twitter/scoot/common/errors"
	"github.com/twitter/scoot/common/log/tags"
	"github.com/twitter/scoot/common/stats"
	"github.com/twitter/scoot/runner"
	"github.com/twitter/scoot/runner/execer"
	"github.com/twitter/scoot/runner/execer/execers"
	"github.com/twitter/scoot/snapshot"
)

// invoke.go: Invoker runs a Scoot command.

// NewInvoker creates an Invoker that will use the supplied helpers
func NewInvoker(
	exec execer.Execer,
	filerMap runner.RunTypeMap,
	output runner.OutputCreator,
	stat stats.StatsReceiver,
	dirMonitor *stats.DirsMonitor,
	rID runner.RunnerID,
	preprocessors []func() error,
	postprocessors []func() error,
	uploader LogUploader,
) *Invoker {
	if stat == nil {
		stat = stats.NilStatsReceiver()
	}
	return &Invoker{exec: exec, filerMap: filerMap, output: output, stat: stat, dirMonitor: dirMonitor, rID: rID, preprocessors: preprocessors, postprocessors: postprocessors, uploader: uploader}
}

// Invoker Runs a Scoot Command by performing the Scoot setup and gathering.
// (E.g., checking out a Snapshot, or saving the Output once it's done)
// Unlike a full Runner, it has no idea of what else is running or has run.
type Invoker struct {
	exec           execer.Execer
	filerMap       runner.RunTypeMap
	output         runner.OutputCreator
	stat           stats.StatsReceiver
	dirMonitor     *stats.DirsMonitor
	rID            runner.RunnerID
	preprocessors  []func() error
	postprocessors []func() error
	uploader       LogUploader
}

// Run runs cmd
// Run will send updates as the process is running to updateCh.
// The RunStatus'es that come out of updateCh will have an empty RunID
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// updateCh will not close until the run is finished running.
func (inv *Invoker) Run(cmd *runner.Command, id runner.RunID) (abortCh chan<- struct{}, updateCh <-chan runner.RunStatus) {
	abortChFull := make(chan struct{})
	memChFull := make(chan execer.ProcessStatus)
	updateChFull := make(chan runner.RunStatus)
	go inv.run(cmd, id, abortChFull, memChFull, updateChFull)
	return abortChFull, updateChFull
}

// Run runs cmd as run id returning the final ProcessStatus
// Run will send updates the process is running to updateCh.
// Run will enforce cmd's Timeout, and will abort cmd if abortCh is signaled.
// Run will not return until the process is not running.
func (inv *Invoker) run(cmd *runner.Command, id runner.RunID, abortCh chan struct{}, memCh chan execer.ProcessStatus, updateCh chan runner.RunStatus) (r runner.RunStatus) {
	log.WithFields(
		log.Fields{
			"runID":  id,
			"tag":    cmd.Tag,
			"jobID":  cmd.JobID,
			"taskID": cmd.TaskID,
		}).Info("*Invoker.run()")
	inv.stat.Gauge(stats.WorkerRunningTask).Update(1)
	defer inv.stat.Gauge(stats.WorkerRunningTask).Update(0)

	taskTimer := inv.stat.Latency(stats.WorkerTaskLatency_ms).Time()

	defer func() {
		taskTimer.Stop()
		updateCh <- r
		close(updateCh)
	}()

	start := time.Now()

	// Records various stages of the run
	// TODO opporunity for consolidation with existing timers and metrics as part of larger refactor
	rts := &runTimes{}
	rts.invokeStart = stamp()

	var co snapshot.Checkout
	checkoutCh := make(chan error)

	// set up pre/postprocessors
	for _, pp := range inv.preprocessors {
		log.Info("running preprocessor")
		if err := pp(); err != nil {
			log.Errorf("Error running preprocessor %s", err)
		}
	}
	defer func() {
		for _, pp := range inv.postprocessors {
			log.Infof("running postprocessor")
			if err := pp(); err != nil {
				log.Errorf("Error running postprocessor %s", err)
			}
		}
	}()

	// Determine RunType from Command SnapshotID
	// This invoker supports RunTypeScoot
	var runType runner.RunType = runner.RunTypeScoot
	if _, ok := inv.filerMap[runType]; !ok {
		return runner.FailedStatus(id,
			errors.NewError(fmt.Errorf("Invoker does not have filer for command of RunType: %s", runType), errors.PreProcessingFailureExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}

	rts.inputStart = stamp()

	// if we are checking out a snapshot, start the timer outside of go routine
	var downloadTimer stats.Latency
	if cmd.SnapshotID != "" {
		downloadTimer = inv.stat.Latency(stats.WorkerDownloadLatency_ms).Time()
		inv.stat.Counter(stats.WorkerDownloads).Inc(1)
	}

	// update local workspace with snapshot
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
			if tmp, err := ioutil.TempDir("", "invoke_nop_checkout"); err != nil {
				checkoutCh <- err
			} else {
				co = snapshot.NewNopCheckout(string(id), tmp)
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

	// wait for checkout to finish (or abort signal)
	select {
	case <-abortCh:
		if err := inv.filerMap[runType].Filer.CancelCheckout(); err != nil {
			log.Errorf("Error canceling checkout: %s", err)
		}
		if err := <-checkoutCh; err != nil {
			log.Errorf("Checkout errored: %s", err)
			// If there was an error there should be no lingering gitdb locks, so return
			// In addition, co should be nil, so failing to return and calling co.Release()
			// will result in a nil pointer dereference
		} else {
			// If there was no error then we need to release this checkout.
			co.Release()
		}
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
			var failedStatus runner.RunStatus
			codeErr, ok := err.(*errors.ExitCodeError)
			switch ok {
			case true:
				// err is of type github.com/twitter/scoot/common/errors.Error
				failedStatus = runner.FailedStatus(id, codeErr,
					tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			default:
				// err is not of type github.com/twitter/scoot/common/errors.Error
				failedStatus = runner.FailedStatus(id, errors.NewError(err, errors.GenericCheckoutFailureExitCode),
					tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
			}

			return failedStatus
		}
		// Checkout is ok, continue with run and when finished release checkout.
		defer co.Release()
		rts.inputEnd = stamp()
	}
	log.WithFields(
		log.Fields{
			"runID":    id,
			"tag":      cmd.Tag,
			"jobID":    cmd.JobID,
			"taskID":   cmd.TaskID,
			"checkout": co.Path(),
		}).Info("Checkout done")

	// setup stdout,stderr output
	stdout, err := inv.output.Create(fmt.Sprintf("%s-stdout", id))
	if err != nil {
		msg := fmt.Sprintf("could not create stdout: %s", err)
		failedStatus := runner.FailedStatus(id, errors.NewError(e.New(msg), errors.LogRefCreationFailureExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		return failedStatus
	}
	defer stdout.Close()

	stderr, err := inv.output.Create(fmt.Sprintf("%s-stderr", id))
	if err != nil {
		msg := fmt.Sprintf("could not create stderr: %s", err)
		failedStatus := runner.FailedStatus(id, errors.NewError(e.New(msg), errors.LogRefCreationFailureExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		return failedStatus
	}
	defer stderr.Close()

	stdlog, err := inv.output.Create(fmt.Sprintf("%s-stdlog", id))
	if err != nil {
		msg := fmt.Sprintf("could not create combined stdout/stderr: %s", err)
		failedStatus := runner.FailedStatus(id, errors.NewError(e.New(msg), errors.LogRefCreationFailureExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		return failedStatus
	}
	defer stdlog.Close()

	marker := "###########################################\n###########################################\n"
	format := "%s\n\nDate: %v\nOut: %s\tErr: %s\tOutErr: %s\tCmd:\n%v\n\n%s\n\n\nSCOOT_CMD_LOG\n"
	header := fmt.Sprintf(format, marker, time.Now(), stdout.URI(), stderr.URI(), stdlog.URI(), cmd, marker)
	// If we wanted to allow optionally, a switch for this would come either at the Worker level
	// (via Invoker -> QueueRunner construction), or the Command level (job requestor specifies in e.g. a PlatformProperty)

	// Processing/setup post checkout before execution
	switch runType {
	case runner.RunTypeScoot:
		stdout.Write([]byte(header))
		stderr.Write([]byte(header))
		stdlog.Write([]byte(header))
	}

	inv.dirMonitor.GetStartSizes() // start monitoring directory sizes

	// start running the command
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
		MemCh:   memCh,
		LogTags: cmd.LogTags,
	})
	if err != nil {
		msg := fmt.Sprintf("could not exec: %s", err)
		failedStatus := runner.FailedStatus(id, errors.NewError(e.New(msg), errors.CouldNotExecExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		return failedStatus
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
	var runStatus runner.RunStatus

	// Wait for process to complete (or cancel if we're told to)
	select {
	case <-abortCh:
		stdout.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask aborted: %v", marker, cmd.String())))
		stderr.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask aborted: %v", marker, cmd.String())))
		stdlog.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask aborted: %v", marker, cmd.String())))
		p.Abort()
		return runner.AbortStatus(id,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	case <-timeoutCh:
		stdout.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask exceeded timeout %v: %v", marker, cmd.Timeout, cmd.String())))
		stderr.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask exceeded timeout %v: %v", marker, cmd.Timeout, cmd.String())))
		stdlog.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\nTask exceeded timeout %v: %v", marker, cmd.Timeout, cmd.String())))
		inv.stat.Counter(stats.WorkerTimeouts).Inc(1)
		p.Abort()
		log.WithFields(
			log.Fields{
				"cmd":    cmd.String(),
				"tag":    cmd.Tag,
				"jobID":  cmd.JobID,
				"taskID": cmd.TaskID,
			}).Error("Run timedout")
		runStatus = runner.TimeoutStatus(id,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	case st := <-memCh:
		stdout.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\n%v", marker, st.Error)))
		stderr.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\n%v", marker, st.Error)))
		stdlog.Write([]byte(fmt.Sprintf("\n\n%s\n\nFAILED\n\n%v", marker, st.Error)))
		log.WithFields(
			log.Fields{
				"runID":    id,
				"cmd":      cmd.String(),
				"tag":      cmd.Tag,
				"jobID":    cmd.JobID,
				"taskID":   cmd.TaskID,
				"status":   st,
				"checkout": co.Path(),
			}).Errorf(st.Error)
		inv.stat.Counter(stats.WorkerMemoryCapExceeded).Inc(1)
		runStatus = getPostExecRunStatus(st, id, cmd)
		runStatus.Error = st.Error
	case st := <-processCh:
		// Process has completed
		log.WithFields(
			log.Fields{
				"runID":    id,
				"cmd":      cmd.String(),
				"tag":      cmd.Tag,
				"jobID":    cmd.JobID,
				"taskID":   cmd.TaskID,
				"status":   st,
				"checkout": co.Path(),
			}).Info("Run done")
		runStatus = getPostExecRunStatus(st, id, cmd)
		if runStatus.State == runner.FAILED {
			return runStatus
		}
	}

	// record command's disk usage for the monitored directories
	inv.dirMonitor.GetEndSizes()
	inv.dirMonitor.RecordSizeStats(inv.stat)

	// the command is no longer running, post process the results
	rts.execEnd = stamp()
	rts.outputStart = stamp()
	var stderrUrl, stdoutUrl string
	// only upload logs to a permanent location if a log uploader is initialized
	if inv.uploader != nil {
		defer inv.stat.Latency(stats.WorkerUploadLatency_ms).Time().Stop()

		// generate a unique id that's appended to the job id to create a unique identifier for logs
		logUid, _ := uuid.NewV4()
		stdlogName := "stdlog"
		stderrName := "stderr"
		stdoutName := "stdout"

		// upload stdlog
		logId := fmt.Sprintf("%s_%s/%s", cmd.JobID, logUid, stdlogName)
		_, isAborted := inv.uploadLog(logId, stdlog.AsFile(), abortCh)
		if isAborted {
			return runner.AbortStatus(id, tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})

		}

		// upload stderr
		// TODO: remove when we transition to using only stdlog in run status
		logId = fmt.Sprintf("%s_%s/%s", cmd.JobID, logUid, stderrName)
		stderrUrl, isAborted = inv.uploadLog(logId, stderr.AsFile(), abortCh)
		if isAborted {
			return runner.AbortStatus(id, tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		}

		// upload stdout
		// TODO: remove when we transition to using only stdlog in run status
		logId = fmt.Sprintf("%s_%s/%s", cmd.JobID, logUid, stdoutName)
		stdoutUrl, isAborted = inv.uploadLog(logId, stdout.AsFile(), abortCh)
		if isAborted {
			return runner.AbortStatus(id, tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		}
		// Note: stdout/stderr refs are only modified when logs are successfully uploaded to storage
		runStatus.StderrRef = stderrUrl
		runStatus.StdoutRef = stdoutUrl
	} else {
		log.Infof("log uploader not initialized, skipping logs upload to storage")
		// Return local std stream refs if log uploader is not initialized
		runStatus.StderrRef = stderr.URI()
		runStatus.StdoutRef = stdout.URI()
	}
	rts.outputEnd = stamp()
	rts.invokeEnd = stamp()
	return runStatus
}

func (inv *Invoker) uploadLog(logId, filepath string, abortCh chan struct{}) (string, bool) {
	var url string
	var err error
	uploadCancelCh := make(chan struct{})
	uploadCh := make(chan error)

	defer inv.stat.Latency(stats.WorkerLogUploadLatency_ms).Time().Stop()
	go func() {
		url, err = inv.uploader.UploadLog(logId, filepath, uploadCancelCh)
		uploadCh <- err
	}()
	select {
	case abort := <-abortCh:
		uploadCancelCh <- abort
		return "", true
	case err := <-uploadCh:
		if err != nil {
			inv.stat.Counter(stats.WorkerLogUploadFailures).Inc(1)
			log.Error(err)
			return "", false
		}
		inv.stat.Counter(stats.WorkerUploads).Inc(1)
		return url, false
	}
}

func getPostExecRunStatus(st execer.ProcessStatus, id runner.RunID, cmd *runner.Command) (runStatus runner.RunStatus) {
	switch st.State {
	case execer.COMPLETE:
		runStatus = runner.CompleteStatus(id, "", st.ExitCode,
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
		if st.Error != "" {
			runStatus.Error = st.Error
		}
	case execer.FAILED:
		// use the exit code from process status if present, otherwise use default exit code
		ec := errors.ExitCode(errors.PostExecFailureExitCode)
		if int(st.ExitCode) != 0 {
			ec = st.ExitCode
		}
		msg := fmt.Sprintf("error execing: %s", st.Error)
		runStatus = runner.FailedStatus(id, errors.NewError(e.New(msg), ec),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	default:
		msg := "unexpected exec state"
		runStatus = runner.FailedStatus(id, errors.NewError(e.New(msg), errors.PostExecFailureExitCode),
			tags.LogTags{JobID: cmd.JobID, TaskID: cmd.TaskID, Tag: cmd.Tag})
	}
	return runStatus
}

// Tracking timestamps for stages of an invoker run.
// Values are only set with non-zero Time when stage has completed successfully.
type runTimes struct {
	invokeStart           time.Time
	invokeEnd             time.Time
	inputStart            time.Time
	actionCacheCheckStart time.Time
	actionCacheCheckEnd   time.Time
	actionFetchStart      time.Time
	actionFetchEnd        time.Time
	commandFetchStart     time.Time
	commandFetchEnd       time.Time
	inputEnd              time.Time
	execStart             time.Time
	execEnd               time.Time
	outputStart           time.Time
	outputEnd             time.Time
	queuedTime            time.Time // set by scheduler and must be populated e.g. by task metadata
}

// Wrapper around time values to encourage "stamp()" usage so it's harder to lose track of runTimes fields.
// Longer term, we should refactor the Invoker so the checkout/exec/upload phases are
// separated from the implementation logic, which will allow these to be recorded clearly
func stamp() time.Time {
	return time.Now()
}
