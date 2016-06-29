package runner

// Helper functions to create ProcessStatus

func ErrorStatus(runId RunId, err error) (r ProcessStatus) {
	r.RunId = runId
	r.State = FAILED
	r.Error = err.Error()
	return r
}

func RunningStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	r.State = RUNNING
	return r
}

func CompleteStatus(runId RunId, stdoutRef string, stderrRef string, exitCode int) (r ProcessStatus) {
	r.RunId = runId
	r.State = COMPLETE
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	r.ExitCode = exitCode
	return r
}
