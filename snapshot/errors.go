package snapshot

type pathError struct {
	underlying error
}

func (s *pathError) Error() string {
	return s.underlying.Error()
}

func (s *pathError) PathError() {}
