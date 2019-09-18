// Package allocator provides tools for centralized management
// of a common resource by multiple clients.
package allocator

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// AbstractAllocator controls access to an abstract pool of resources of a specified capacity.
// Abstract in this sense means the resources don't actually represent a physical resource
// (such as disk, memory, etc), but a means of limiting overall concurrent usage by a set of clients.
type AbstractAllocator struct {
	mu        sync.Mutex
	capacity  int64
	allocated int64
}

// NewAbstractAllocator returns a new *AbstractAllocator initialized with a set capacity.
// Returns an error if capacity is < 0. Typical usage of this allocator:
//	a := NewAbstractAllocator(1024)
//	r, err := a.Alloc(64)
//	// handle err
//	defer r.Release()
func NewAbstractAllocator(c int64) (*AbstractAllocator, error) {
	if c < 0 {
		return nil, fmt.Errorf("invalid capacity %d < 0", c)
	}
	return &AbstractAllocator{capacity: c}, nil
}

// Alloc returns a resource of an indicated size or an error.
// If error is nil, a non-nil *AbstractResource is returned, which the client must
// Release when finished, or a resource leak will result.
func (a *AbstractAllocator) Alloc(size int64) (*AbstractResource, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if size < 0 {
		return nil, fmt.Errorf("invalid size %d < 0", size)
	}
	if a.allocated+size > a.capacity {
		return nil, fmt.Errorf(
			"alloc request: %d exceeds capacity: %d (current allocation: %d)", size, a.capacity, a.allocated)
	}
	a.allocated += size
	return &AbstractResource{size: size, a: a}, nil
}

// WaitAlloc attempts to perform an Alloc up until any supplied context has been cancelled.
// WaitAlloc will immediately attempt an Alloc and, if unable, will retry at an unspecified
// rate until the context has cancelled or the Alloc has completed successfully.
func (a *AbstractAllocator) WaitAlloc(ctx context.Context, size int64) (*AbstractResource, error) {
	r, err := a.Alloc(size)
	if err == nil {
		return r, nil
	}

	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-t.C:
			r, err = a.Alloc(size)
			if err == nil {
				return r, nil
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Release returns a given resource back to the allocator. Releasing a nil
// or previously released resource does nothing.
func (a *AbstractAllocator) release(r *AbstractResource) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r != nil {
		a.allocated -= r.size
		if a.allocated < 0 {
			a.allocated = 0
		}
		// unset the resource to prevent accidental double-releasing
		r.size = 0
	}
}

// AbstractResource represents some amount of resources granted
// by an AbstractAllocator and held by a client.
type AbstractResource struct {
	size int64
	a    *AbstractAllocator
}

// Release returns a given resource back to the allocator that created this
// resource. Releasing a nil or previously released resource does nothing.
func (r *AbstractResource) Release() {
	if r != nil && r.a != nil {
		r.a.release(r)
	}
}
