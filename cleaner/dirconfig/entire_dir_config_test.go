package dirconfig

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testCase struct {
	name        string
	maxUsageKB  uint64
	content     string
	wantCleaned bool
}

func TestEntireDirConfigCleanDir(t *testing.T) {
	testCases := []testCase{
		{"Under Limit", 1024, "test under limit content", false},
		{"Over Limit", 0, "test over limit content", true},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// create dir to be cleaned
			tempDir, err := ioutil.TempDir("", "test-dir-to-clean-*")
			fmt.Println(tempDir)

			if err != nil {
				t.Fatalf("failed to create tempDir: %s", err)
			}
			defer os.RemoveAll(tempDir)

			// create nested directory
			tempNestedDir := filepath.Join(tempDir, "nested")
			err = os.Mkdir(tempNestedDir, os.ModePerm)
			if err != nil {
				t.Fatalf("Could not create nested dir %s: %s", tempNestedDir, err)
			}

			// create file in nested dir
			tempFile := filepath.Join(tempNestedDir, "test.txt")
			err = ioutil.WriteFile(tempFile, []byte(tc.content), os.ModePerm)
			if err != nil {
				t.Fatalf("Could not write file to %s: %s", tempFile, err)
			}

			// execute CleanDir
			dc := EntireDirConfig{tempDir, tc.maxUsageKB}
			if err := dc.CleanDir(); err != nil {
				t.Errorf("CleanDir failed:  %s", err)
			}

			// assert cleaned
			_, err = os.Stat(tempDir)
			assert.Equal(t, tc.wantCleaned, err != nil, "Invalid directory status: %v", err)
		})
	}
}
