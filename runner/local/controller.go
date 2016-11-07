package local

type Controller interface {
	Run(cmd *runner.Command) (runner.ProcessStatus, error)
	Abort(run runner.RunId) (runner.ProcessStatus, error)
}
