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
