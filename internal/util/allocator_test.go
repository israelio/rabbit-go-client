package util

import (
	"sync"
	"testing"
)

// IntAllocator allocates integer IDs (e.g., for channel IDs)
type IntAllocator struct {
	min, max int
	free     map[int]bool
	mu       sync.Mutex
}

// NewIntAllocator creates a new integer allocator
func NewIntAllocator(min, max int) *IntAllocator {
	free := make(map[int]bool, max-min+1)
	for i := min; i <= max; i++ {
		free[i] = true
	}
	return &IntAllocator{
		min:  min,
		max:  max,
		free: free,
	}
}

// Allocate allocates a new integer
func (a *IntAllocator) Allocate() (int, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	for i := a.min; i <= a.max; i++ {
		if a.free[i] {
			delete(a.free, i)
			return i, true
		}
	}
	return 0, false
}

// Free releases an integer back to the pool
func (a *IntAllocator) Free(i int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if i < a.min || i > a.max {
		return false
	}
	if a.free[i] {
		return false // Already free
	}
	a.free[i] = true
	return true
}

// Reserve marks an integer as allocated
func (a *IntAllocator) Reserve(i int) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if i < a.min || i > a.max {
		return false
	}
	if !a.free[i] {
		return false // Already allocated
	}
	delete(a.free, i)
	return true
}

// Available returns number of available integers
func (a *IntAllocator) Available() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.free)
}

// TestIntAllocatorBasic tests basic allocation and freeing
func TestIntAllocatorBasic(t *testing.T) {
	alloc := NewIntAllocator(1, 10)

	// Allocate
	id1, ok := alloc.Allocate()
	if !ok {
		t.Fatal("First allocation failed")
	}
	if id1 < 1 || id1 > 10 {
		t.Errorf("Allocated ID out of range: %d", id1)
	}

	// Allocate another
	id2, ok := alloc.Allocate()
	if !ok {
		t.Fatal("Second allocation failed")
	}
	if id1 == id2 {
		t.Error("Allocated same ID twice")
	}

	// Free
	if !alloc.Free(id1) {
		t.Error("Free failed")
	}

	// Allocate again, should get freed ID
	id3, ok := alloc.Allocate()
	if !ok {
		t.Fatal("Third allocation failed")
	}
	if id3 != id1 {
		t.Logf("Got different ID after free (expected %d, got %d) - acceptable", id1, id3)
	}
}

// TestIntAllocatorExhaustion tests exhausting the allocator
func TestIntAllocatorExhaustion(t *testing.T) {
	alloc := NewIntAllocator(1, 5)

	allocated := make([]int, 0, 5)

	// Allocate all
	for i := 0; i < 5; i++ {
		id, ok := alloc.Allocate()
		if !ok {
			t.Fatalf("Allocation %d failed", i)
		}
		allocated = append(allocated, id)
	}

	// Try to allocate one more
	_, ok := alloc.Allocate()
	if ok {
		t.Error("Should have failed to allocate when exhausted")
	}

	// Free one
	if !alloc.Free(allocated[0]) {
		t.Error("Free failed")
	}

	// Should be able to allocate again
	_, ok = alloc.Allocate()
	if !ok {
		t.Error("Allocation after free failed")
	}
}

// TestIntAllocatorReserve tests reserving specific IDs
func TestIntAllocatorReserve(t *testing.T) {
	alloc := NewIntAllocator(1, 10)

	// Reserve specific ID
	if !alloc.Reserve(5) {
		t.Error("Reserve failed")
	}

	// Try to allocate - should skip reserved ID
	allocated := make(map[int]bool)
	for i := 0; i < 9; i++ {
		id, ok := alloc.Allocate()
		if !ok {
			t.Fatalf("Allocation %d failed", i)
		}
		if id == 5 {
			t.Error("Allocated reserved ID")
		}
		allocated[id] = true
	}

	// Should have allocated all except 5
	if len(allocated) != 9 {
		t.Errorf("Allocated count: got %d, want 9", len(allocated))
	}
}

// TestIntAllocatorConcurrent tests concurrent allocation
func TestIntAllocatorConcurrent(t *testing.T) {
	alloc := NewIntAllocator(1, 100)

	numGoroutines := 10
	allocationsPerGoroutine := 5

	var wg sync.WaitGroup
	allocated := make(chan int, numGoroutines*allocationsPerGoroutine)
	errors := make(chan error, numGoroutines*allocationsPerGoroutine)

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for j := 0; j < allocationsPerGoroutine; j++ {
				id, ok := alloc.Allocate()
				if !ok {
					errors <- nil
				} else {
					allocated <- id
				}
			}
		}()
	}

	wg.Wait()
	close(allocated)
	close(errors)

	// Check for duplicates
	seen := make(map[int]bool)
	for id := range allocated {
		if seen[id] {
			t.Errorf("ID %d allocated multiple times", id)
		}
		seen[id] = true
	}
}

// TestIntAllocatorAvailable tests Available method
func TestIntAllocatorAvailable(t *testing.T) {
	alloc := NewIntAllocator(1, 10)

	if avail := alloc.Available(); avail != 10 {
		t.Errorf("Initial available: got %d, want 10", avail)
	}

	id, _ := alloc.Allocate()

	if avail := alloc.Available(); avail != 9 {
		t.Errorf("After allocation: got %d, want 9", avail)
	}

	alloc.Free(id)

	if avail := alloc.Available(); avail != 10 {
		t.Errorf("After free: got %d, want 10", avail)
	}
}

// TestIntAllocatorInvalidFree tests freeing invalid IDs
func TestIntAllocatorInvalidFree(t *testing.T) {
	alloc := NewIntAllocator(1, 10)

	// Free ID outside range
	if alloc.Free(0) {
		t.Error("Should not free ID below range")
	}
	if alloc.Free(11) {
		t.Error("Should not free ID above range")
	}

	// Free already free ID
	id, _ := alloc.Allocate()
	alloc.Free(id)
	if alloc.Free(id) {
		t.Error("Should not free already free ID")
	}
}

// BenchmarkIntAllocator benchmarks allocation performance
func BenchmarkIntAllocator(b *testing.B) {
	alloc := NewIntAllocator(1, 1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, ok := alloc.Allocate()
		if ok {
			alloc.Free(id)
		}
	}
}
