package allocator

import (
	"context"
	"testing"
	"time"
)

func TestSimpleAllocs(t *testing.T) {
	_, err := NewAbstractAllocator(-1)
	if err == nil {
		t.Fatal("expected error to allocate with negative capacity")
	}

	a, err := NewAbstractAllocator(42069)
	if err != nil {
		t.Fatal(err)
	}

	// normal allocation
	r1, err := a.Alloc(42068)
	if err != nil {
		t.Fatalf("error from Alloc: %s", err)
	}

	// try to allocate beyond capacity
	_, err = a.Alloc(99999)
	if err == nil {
		t.Fatal("expected to fail to allocate beyond capacity")
	}

	// release and verify resources available
	r1.Release()
	r2, err := a.Alloc(10000)
	if err != nil {
		t.Fatalf("Error from Alloc: %s", err)
	}

	// verify expected allocated amount
	if a.allocated != 10000 {
		t.Fatalf("allocated amount doesn't match, got: %d, want: %d", a.allocated, 10000)
	}

	// verify double release does nothing
	r1.Release()
	if a.allocated != 10000 {
		t.Fatalf("allocated amount doesn't match, got: %d, want: %d", a.allocated, 10000)
	}

	// verify we can't release below 0
	r2.Release()
	r3 := &AbstractResource{size: 99999999, a: a}
	r3.Release()
	if a.allocated != 0 {
		t.Fatalf("allocated amount doesn't match, got: %d, want: %d", a.allocated, 0)
	}

	// negative allocation
	_, err = a.Alloc(-1)
	if err == nil {
		t.Fatal("expected error to allocate negative quantity")
	}
}

func TestWaitAllocs(t *testing.T) {
	a, err := NewAbstractAllocator(10)
	if err != nil {
		t.Fatal(err)
	}

	// verify behavior as regular alloc
	ctx := context.Background()
	r1, err := a.WaitAlloc(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}

	ctx1, cancel1 := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel1()
	r2, err := a.WaitAlloc(ctx1, 10)
	if err == nil {
		t.Fatal("expected to fail WaitAlloc with short deadline")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	go func() {
		time.Sleep(100 * time.Millisecond)
		r1.Release()
	}()
	r2, err = a.WaitAlloc(ctx2, 10)
	if err != nil {
		t.Fatal(err)
	}
	r2.Release()
}
