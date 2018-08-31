package common

import (
	"fmt"
	"os"
	"strings"
)

func GetFirstGopath() (string, error) {
	gp, ok := os.LookupEnv("GOPATH")
	if !ok {
		return "", fmt.Errorf("Error: GOPATH not set")
	}
	s := strings.Split(gp, ":")
	return s[0], nil
}

// Splits a comma separated string consisting of key value pairs,
// e.g. "k1=v1,k2=v2", into a map
func SplitCommaSepToMap(commaSepString string) map[string]string {
	m := make(map[string]string)
	for _, pair := range strings.Split(commaSepString, ",") {
		if pair == "" {
			continue
		}
		kv := strings.Split(pair, "=")
		if len(kv) != 2 {
			continue
		}
		m[kv[0]] = kv[1]
	}
	return m
}
