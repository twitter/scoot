package errors

type ExitCodeError struct {
	code ExitCode
	error
}

func NewError(err error, exitCode ExitCode) *ExitCodeError {
	if err == nil {
		return nil
	}
	return &ExitCodeError{exitCode, err}
}

func (e *ExitCodeError) GetExitCode() ExitCode {
	if e == nil {
		return 0
	}
	return e.code
}
