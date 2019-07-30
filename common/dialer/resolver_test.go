package dialer

import (
	"os"
	"testing"
)

func TestConstantResolver(t *testing.T) {
	addr := "127.0.0.1:30"
	c := NewConstantResolver(addr)

	res, err := c.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if res != addr {
		t.Fatalf("got: %s, want: %s", res, addr)
	}

	slice, err := c.ResolveMany(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(slice) != 1 || slice[0] != addr {
		t.Fatalf("got: %s, want: %s", slice, []string{addr})
	}
}

func TestEnvResolver(t *testing.T) {
	addr := "127.0.0.1:30"
	os.Setenv("SCOOT_TEST_ADDR", addr)
	e := NewEnvResolver("SCOOT_TEST_ADDR")

	res, err := e.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if res != addr {
		t.Fatalf("got: %s, want: %s", res, addr)
	}

	slice, err := e.ResolveMany(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(slice) != 1 || slice[0] != addr {
		t.Fatalf("got: %s, want: %s", slice, []string{addr})
	}
}

func TestCompositeResolver(t *testing.T) {
	addr := "127.0.0.1:30"
	addr2 := "1.2.3.4:99"
	c := NewConstantResolver(addr)
	e := NewEnvResolver("SCOOT_TEST_ADDR")
	cr := NewCompositeResolver(e, c)

	res, err := cr.Resolve()
	if err != nil {
		t.Fatal(err)
	}
	if res != addr {
		t.Fatalf("got: %s, want: %s", res, addr)
	}

	os.Setenv("SCOOT_TEST_ADDR", addr2)

	slice, err := e.ResolveMany(0)
	if err != nil {
		t.Fatal(err)
	}
	if len(slice) != 1 || slice[0] != addr2 {
		t.Fatalf("got: %s, want: %s", slice, []string{addr})
	}
}
