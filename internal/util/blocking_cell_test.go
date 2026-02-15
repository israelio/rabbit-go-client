package util

import (
	"context"
	"errors"
	"testing"
	"time"
)

// BlockingCell is a one-shot blocking container for a value
// Equivalent to Java's BlockingCell
type BlockingCell struct {
	valueChan chan interface{}
	set       bool
}

// NewBlockingCell creates a new blocking cell
func NewBlockingCell() *BlockingCell {
	return &BlockingCell{
		valueChan: make(chan interface{}, 1),
	}
}

// Set sets the value in the cell
func (c *BlockingCell) Set(value interface{}) error {
	if c.set {
		return errors.New("cell already set")
	}
	c.set = true
	c.valueChan <- value
	return nil
}

// Get gets the value from the cell, blocking if not yet set
func (c *BlockingCell) Get() interface{} {
	return <-c.valueChan
}

// GetWithTimeout gets the value with a timeout
func (c *BlockingCell) GetWithTimeout(timeout time.Duration) (interface{}, error) {
	select {
	case value := <-c.valueChan:
		return value, nil
	case <-time.After(timeout):
		return nil, errors.New("timeout")
	}
}

// GetWithContext gets the value with a context
func (c *BlockingCell) GetWithContext(ctx context.Context) (interface{}, error) {
	select {
	case value := <-c.valueChan:
		return value, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// TestBlockingCellBasic tests basic set/get
func TestBlockingCellBasic(t *testing.T) {
	cell := NewBlockingCell()

	// Set value
	if err := cell.Set(42); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get value
	value := cell.Get()
	if value != 42 {
		t.Errorf("Get: got %v, want 42", value)
	}
}

// TestBlockingCellBlocking tests that Get blocks until Set
func TestBlockingCellBlocking(t *testing.T) {
	cell := NewBlockingCell()

	done := make(chan struct{})
	var value interface{}

	// Start goroutine that blocks on Get
	go func() {
		value = cell.Get()
		close(done)
	}()

	// Wait a bit to ensure goroutine is blocking
	time.Sleep(100 * time.Millisecond)

	// Set value
	if err := cell.Set("test"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Wait for Get to unblock
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Get did not unblock after Set")
	}

	if value != "test" {
		t.Errorf("Value: got %v, want test", value)
	}
}

// TestBlockingCellTimeout tests timeout behavior
func TestBlockingCellTimeout(t *testing.T) {
	cell := NewBlockingCell()

	// Try to get with short timeout (no value set)
	value, err := cell.GetWithTimeout(100 * time.Millisecond)
	if err == nil {
		t.Error("Should have timed out")
	}
	if value != nil {
		t.Errorf("Value should be nil on timeout, got %v", value)
	}
}

// TestBlockingCellTimeoutSuccess tests get with timeout succeeds
func TestBlockingCellTimeoutSuccess(t *testing.T) {
	cell := NewBlockingCell()

	// Set value in background
	go func() {
		time.Sleep(50 * time.Millisecond)
		cell.Set("success")
	}()

	// Get with longer timeout
	value, err := cell.GetWithTimeout(time.Second)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if value != "success" {
		t.Errorf("Value: got %v, want success", value)
	}
}

// TestBlockingCellContext tests context cancellation
func TestBlockingCellContext(t *testing.T) {
	cell := NewBlockingCell()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Try to get with context (no value set)
	value, err := cell.GetWithContext(ctx)
	if err == nil {
		t.Error("Should have been cancelled")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("Error type: got %v, want context.DeadlineExceeded", err)
	}
	if value != nil {
		t.Errorf("Value should be nil on cancellation, got %v", value)
	}
}

// TestBlockingCellDoubleSet tests that setting twice fails
func TestBlockingCellDoubleSet(t *testing.T) {
	cell := NewBlockingCell()

	// First set
	if err := cell.Set("first"); err != nil {
		t.Fatalf("First Set failed: %v", err)
	}

	// Second set should fail
	err := cell.Set("second")
	if err == nil {
		t.Error("Second Set should have failed")
	}
}

// TestBlockingCellMultipleGetters tests multiple goroutines getting
func TestBlockingCellMultipleGetters(t *testing.T) {
	cell := NewBlockingCell()

	numGetters := 5
	results := make(chan interface{}, numGetters)

	// Start multiple getters
	for i := 0; i < numGetters; i++ {
		go func() {
			value := cell.Get()
			results <- value
		}()
	}

	// Wait to ensure all are blocking
	time.Sleep(100 * time.Millisecond)

	// Set value
	cell.Set("shared")

	// Only one should get the value
	select {
	case value := <-results:
		if value != "shared" {
			t.Errorf("Value: got %v, want shared", value)
		}
	case <-time.After(time.Second):
		t.Fatal("No getter received value")
	}

	// Others may or may not receive depending on channel buffer
	// This is expected behavior for one-shot cell
}

// TestBlockingCellWithError tests storing error values
func TestBlockingCellWithError(t *testing.T) {
	cell := NewBlockingCell()

	expectedErr := errors.New("test error")

	// Set error
	if err := cell.Set(expectedErr); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get error
	value := cell.Get()
	err, ok := value.(error)
	if !ok {
		t.Fatalf("Value is not an error: %T", value)
	}
	if err.Error() != expectedErr.Error() {
		t.Errorf("Error: got %v, want %v", err, expectedErr)
	}
}

// TestBlockingCellWithStruct tests storing struct values
func TestBlockingCellWithStruct(t *testing.T) {
	type Result struct {
		Code    int
		Message string
	}

	cell := NewBlockingCell()

	expected := Result{Code: 200, Message: "OK"}

	// Set struct
	if err := cell.Set(expected); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Get struct
	value := cell.Get()
	result, ok := value.(Result)
	if !ok {
		t.Fatalf("Value is not Result: %T", value)
	}
	if result != expected {
		t.Errorf("Result: got %+v, want %+v", result, expected)
	}
}

// BenchmarkBlockingCell benchmarks set and get operations
func BenchmarkBlockingCell(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			cell := NewBlockingCell()

			go func() {
				cell.Set(42)
			}()

			value := cell.Get()
			if value != 42 {
				b.Errorf("Got %v, want 42", value)
			}
		}
	})
}
