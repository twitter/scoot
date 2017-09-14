package helpers

func CopyStringToPointer(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func CopyPointerToString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func CopyPointerToInt32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}
