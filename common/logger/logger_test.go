package logger

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestLogUtils(t *testing.T) {

	Fatal("Fatal message without param")
	Fatalf("%s, %f", "Fatal message with param", 2.3)

	Info("Info message without param", 8)
	Infof("Info message with param %d", 10)

	SetLevel(FATAL_LEVEL)
	Info("Info message without param")

	lf := getLogFilename()
	fmt.Println("messages written to:", lf)

	file, err := os.Open(lf)
	if err != nil {
		t.Fatalf("Couldn't find log file:%s", lf)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Scan()
	line := scanner.Text()

	if !strings.Contains(line, "Fatal message without param") {
		t.Errorf("Expected: 'FATAL;<datetime, line number>: Fatal message without param' got '%s' in %s\n", line, lf)
	}
	scanner.Scan()
	line = scanner.Text()
	if !strings.Contains(line, "Fatal message with param") ||
		!strings.Contains(line, "FATAL;") {
		t.Errorf("Expected: 'FATAL;<datetime, line number>:Fatal message with param' got '%s' in %s\n", line, lf)
	}
	scanner.Scan()
	line = scanner.Text()
	if !strings.Contains(line, "Info message without param8") ||
		!strings.Contains(line, "INFO;") {
		t.Errorf("Expected: 'Info message without param8' got '%s' in %s\n", line, lf)
	}
	scanner.Scan()
	line = scanner.Text()
	if !strings.Contains(line, "Info message with param") ||
		!strings.Contains(line, "INFO;") {
		t.Errorf("Expected 'Info message with param' got '%s' in %s\n", line, lf)
	}

	if scanner.Scan() {
		line = scanner.Text()
		t.Errorf("Expected last scan to return false (due to end of text), instead got %s", line)
	}
}
