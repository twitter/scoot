package runner

// Helper functions to create ProcessStatus

func ErrorStatus(runId RunId, err error) (r ProcessStatus) {
	r.RunId = runId
	r.Error = err.Error()
	return r
}

func RunningStatus(runId RunId) (r ProcessStatus) {
	r.RunId = runId
	return r
}

func CompletedStatus(runId RunId, stdoutRef string, stderrRef string, exitCode int) (r ProcessStatus) {
	r.RunId = runId
	r.StdoutRef = stdoutRef
	r.StderrRef = stderrRef
	r.ExitCode = exitCode
	return r
}
