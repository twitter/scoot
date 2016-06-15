package perf

import "testing"

func TestNil(t *testing.T) {
	path := UnsafePathJoin(true)
	if path != "" {
		t.FailNow()
	}
}

func TestEmpty(t *testing.T) {
	path := UnsafePathJoin(true, "")
	if path != "" {
		t.FailNow()
	}

	path = UnsafePathJoin(true, "", "")
	if path != "" {
		t.FailNow()
	}
}

func TestOne(t *testing.T) {
	path := UnsafePathJoin(true, "a")
	if path != "a" {
		t.FailNow()
	}

	path = UnsafePathJoin(true, "abc")
	if path != "abc" {
		t.FailNow()
	}
}

func TestTwo(t *testing.T) {
	path := UnsafePathJoin(true, "ab", "cd")
	if path != "ab/cd" {
		t.FailNow()
	}
}

func TestSuccessive(t *testing.T) {
	pathA := UnsafePathJoin(true, "ab", "cd")
	if pathA != "ab/cd" {
		t.FailNow()
	}
	pathB := UnsafePathJoin(true, "ab", "cd")
	if pathB != "ab/cd" {
		t.FailNow()
	}
}

func TestLong(t *testing.T) {
	expectFail := false
	defer func() {
		if r := recover(); r != nil && !expectFail {
			t.Fatalf("Not expecting panic")
		}
	}()

	var root string
	for ii := 0; ii < 4096-2; ii++ {
		root += "x"
	}
	pathA := UnsafePathJoin(true, root, "")
	if pathA != root {
		t.Fatalf("Wrong return val %v", pathA)
	}
	pathB := UnsafePathJoin(true, root, "a")
	if pathB != root+"/a" {
		t.FailNow()
	}

	expectFail = true
	UnsafePathJoin(true, root, "ab")
	t.Fatalf("Expected panic")
}
