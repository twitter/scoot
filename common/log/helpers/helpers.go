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

func CopyIntPointerToInt32Pointer(i *int) *int32 {
	if i == nil {
		return nil
	}
	i2 := int32(*i)
	return &i2
}
