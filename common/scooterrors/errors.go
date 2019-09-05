package scooterrors

type ScootError struct {
	error
	ExitCode int
}

func NewScootError(err error, exitCode int) *ScootError {
	if err == nil {
		return nil
	}
	return &ScootError{err, exitCode}
}

func (s *ScootError) GetExitCode() int {
	if s == nil {
		return 0
	}
	return s.ExitCode
}
