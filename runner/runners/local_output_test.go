package runners

import (
	"os"
	"testing"

	"github.com/twitter/scoot/common/log/hooks"

	log "github.com/sirupsen/logrus"
)

func init() {
	log.AddHook(hooks.NewContextHook())
	logrusLevel, _ := log.ParseLevel("debug")
	log.SetLevel(logrusLevel)
}

func TestLocalOutputCreator(t *testing.T) {
	h, err := NewHttpOutputCreator("")
	if err != nil {
		t.Fatalf("Unable to create output creator: %v", err)
	}
	o, err := h.Create("test-id")
	if err != nil {
		t.Fatalf("Error creating output: %v", err)
	}
	if _, err := os.Stat(o.AsFile()); os.IsNotExist(err) {
		t.Fatalf("Didn't create output, file %v does not exist. Err: %v", o.AsFile(), err)
	}
}

func TestLocalOutputCreatorNonexistentTempDir(t *testing.T) {
	h, err := NewHttpOutputCreator("")
	if err != nil {
		t.Fatalf("Unable to create output creator: %v", err)
	}
	o, err := h.Create("test-id") // should recreate td
	if err != nil {
		t.Fatalf("Error creating output: %v", err)
	}
	if _, err := os.Stat(o.AsFile()); os.IsNotExist(err) {
		t.Fatalf("Didn't create output, file %v does not exist. Err: %v", o.AsFile(), err)
	}
}
