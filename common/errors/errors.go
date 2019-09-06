package errors

type Error struct {
	ExitCode int
	error
}

func NewError(err error, exitCode int) *Error {
	if err == nil {
		return nil
	}
	return &Error{exitCode, err}
}

func (e *Error) GetExitCode() int {
	if e == nil {
		return 0
	}
	return e.ExitCode
}
