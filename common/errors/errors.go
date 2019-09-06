package errors

type Error struct {
	code ExitCode
	error
}

func NewError(err error, exitCode ExitCode) *Error {
	if err == nil {
		return nil
	}
	return &Error{exitCode, err}
}

func (e *Error) GetExitCode() ExitCode {
	if e == nil {
		return 0
	}
	return e.code
}
