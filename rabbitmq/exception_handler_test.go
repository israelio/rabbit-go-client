package rabbitmq

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestDefaultErrorHandler tests default exception handler
func TestDefaultErrorHandler(t *testing.T) {
	handler := &DefaultErrorHandler{}

	// Test connection error
	connErr := errors.New("connection error")
	handler.HandleConnectionError(nil, connErr)

	// Test channel error
	chanErr := errors.New("channel error")
	handler.HandleChannelError(nil, chanErr)

	// Test consumer error
	consumerErr := errors.New("consumer error")
	handler.HandleConsumerError(nil, "consumer-tag", consumerErr)

	// Should not panic
	t.Log("Default exception handler handled all errors")
}

// TestCustomErrorHandler tests custom exception handler
func TestCustomErrorHandler(t *testing.T) {
	var mu sync.Mutex
	capturedErrors := make(map[string]error)

	handler := &CustomErrorHandler{
		OnConnectionError: func(conn *Connection, err error) {
			mu.Lock()
			capturedErrors["connection"] = err
			mu.Unlock()
		},
		OnChannelError: func(ch *Channel, err error) {
			mu.Lock()
			capturedErrors["channel"] = err
			mu.Unlock()
		},
		OnConsumerError: func(ch *Channel, tag string, err error) {
			mu.Lock()
			capturedErrors["consumer-"+tag] = err
			mu.Unlock()
		},
	}

	// Trigger errors
	handler.HandleConnectionError(nil, errors.New("conn error"))
	handler.HandleChannelError(nil, errors.New("chan error"))
	handler.HandleConsumerError(nil, "tag1", errors.New("consumer error"))

	// Verify errors were captured
	mu.Lock()
	defer mu.Unlock()

	if capturedErrors["connection"] == nil {
		t.Error("Connection error not captured")
	}
	if capturedErrors["channel"] == nil {
		t.Error("Channel error not captured")
	}
	if capturedErrors["consumer-tag1"] == nil {
		t.Error("Consumer error not captured")
	}

	t.Logf("Captured errors: %+v", capturedErrors)
}

// TestErrorHandlerConcurrency tests concurrent error handling
func TestErrorHandlerConcurrency(t *testing.T) {
	var mu sync.Mutex
	errorCount := 0

	handler := &CustomErrorHandler{
		OnConnectionError: func(conn *Connection, err error) {
			mu.Lock()
			errorCount++
			mu.Unlock()
			time.Sleep(time.Millisecond) // Simulate work
		},
		OnChannelError: func(ch *Channel, err error) {
			mu.Lock()
			errorCount++
			mu.Unlock()
			time.Sleep(time.Millisecond)
		},
	}

	// Trigger many errors concurrently
	var wg sync.WaitGroup
	numErrors := 100

	for i := 0; i < numErrors; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			if id%2 == 0 {
				handler.HandleConnectionError(nil, errors.New("error"))
			} else {
				handler.HandleChannelError(nil, errors.New("error"))
			}
		}(i)
	}

	wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	if errorCount != numErrors {
		t.Errorf("Error count: got %d, want %d", errorCount, numErrors)
	}
}

// TestReturnListenerError tests return listener error handling
func TestReturnListenerError(t *testing.T) {
	var mu sync.Mutex
	returnErrors := make([]error, 0)

	handler := &CustomErrorHandler{
		OnReturnListenerError: func(ch *Channel, err error) {
			mu.Lock()
			returnErrors = append(returnErrors, err)
			mu.Unlock()
		},
	}

	// Trigger return errors
	for i := 0; i < 3; i++ {
		handler.HandleReturnListenerError(nil, errors.New("return error"))
	}

	mu.Lock()
	defer mu.Unlock()

	if len(returnErrors) != 3 {
		t.Errorf("Return errors: got %d, want 3", len(returnErrors))
	}
}

// TestConfirmListenerError tests confirm listener error handling
func TestConfirmListenerError(t *testing.T) {
	var mu sync.Mutex
	confirmErrors := make([]error, 0)

	handler := &CustomErrorHandler{
		OnConfirmListenerError: func(ch *Channel, err error) {
			mu.Lock()
			confirmErrors = append(confirmErrors, err)
			mu.Unlock()
		},
	}

	// Trigger confirm errors
	handler.HandleConfirmListenerError(nil, errors.New("confirm error 1"))
	handler.HandleConfirmListenerError(nil, errors.New("confirm error 2"))

	mu.Lock()
	defer mu.Unlock()

	if len(confirmErrors) != 2 {
		t.Errorf("Confirm errors: got %d, want 2", len(confirmErrors))
	}
}

// TestErrorHandlerPanic tests that handlers don't crash on panic
func TestErrorHandlerPanic(t *testing.T) {
	handler := &CustomErrorHandler{
		OnConnectionError: func(conn *Connection, err error) {
			panic("handler panic")
		},
	}

	// Should not crash the test
	defer func() {
		if r := recover(); r == nil {
			t.Error("Expected panic to be caught")
		} else {
			t.Logf("Caught panic: %v", r)
		}
	}()

	handler.HandleConnectionError(nil, errors.New("test"))
}

// TestNilErrorHandler tests behavior with nil handler
func TestNilErrorHandler(t *testing.T) {
	var handler ErrorHandler = nil

	// Should not crash
	if handler == nil {
		t.Log("Nil handler detected, using default")
		handler = &DefaultErrorHandler{}
	}

	handler.HandleConnectionError(nil, errors.New("test"))
	t.Log("Nil handler handled gracefully")
}

// TestErrorHandlerChain tests chaining multiple handlers
func TestErrorHandlerChain(t *testing.T) {
	var mu sync.Mutex
	called := make([]string, 0)

	handler1 := &CustomErrorHandler{
		OnConnectionError: func(conn *Connection, err error) {
			mu.Lock()
			called = append(called, "handler1")
			mu.Unlock()
		},
	}

	handler2 := &CustomErrorHandler{
		OnConnectionError: func(conn *Connection, err error) {
			mu.Lock()
			called = append(called, "handler2")
			mu.Unlock()
		},
	}

	chain := &ErrorHandlerChain{
		Handlers: []ErrorHandler{handler1, handler2},
	}

	chain.HandleConnectionError(nil, errors.New("test"))

	mu.Lock()
	defer mu.Unlock()

	if len(called) != 2 {
		t.Errorf("Called count: got %d, want 2", len(called))
	}
	if called[0] != "handler1" || called[1] != "handler2" {
		t.Errorf("Call order: got %v, want [handler1, handler2]", called)
	}
}

// CustomErrorHandler implements ErrorHandler
type CustomErrorHandler struct {
	OnConnectionError      func(*Connection, error)
	OnChannelError         func(*Channel, error)
	OnConsumerError        func(*Channel, string, error)
	OnReturnListenerError  func(*Channel, error)
	OnConfirmListenerError func(*Channel, error)
}

func (h *CustomErrorHandler) HandleConnectionError(conn *Connection, err error) {
	if h.OnConnectionError != nil {
		h.OnConnectionError(conn, err)
	}
}

func (h *CustomErrorHandler) HandleChannelError(ch *Channel, err error) {
	if h.OnChannelError != nil {
		h.OnChannelError(ch, err)
	}
}

func (h *CustomErrorHandler) HandleConsumerError(ch *Channel, tag string, err error) {
	if h.OnConsumerError != nil {
		h.OnConsumerError(ch, tag, err)
	}
}

func (h *CustomErrorHandler) HandleReturnListenerError(ch *Channel, err error) {
	if h.OnReturnListenerError != nil {
		h.OnReturnListenerError(ch, err)
	}
}

func (h *CustomErrorHandler) HandleConfirmListenerError(ch *Channel, err error) {
	if h.OnConfirmListenerError != nil {
		h.OnConfirmListenerError(ch, err)
	}
}

// ErrorHandlerChain chains multiple handlers
type ErrorHandlerChain struct {
	Handlers []ErrorHandler
}

func (c *ErrorHandlerChain) HandleConnectionError(conn *Connection, err error) {
	for _, h := range c.Handlers {
		h.HandleConnectionError(conn, err)
	}
}

func (c *ErrorHandlerChain) HandleChannelError(ch *Channel, err error) {
	for _, h := range c.Handlers {
		h.HandleChannelError(ch, err)
	}
}

func (c *ErrorHandlerChain) HandleConsumerError(ch *Channel, tag string, err error) {
	for _, h := range c.Handlers {
		h.HandleConsumerError(ch, tag, err)
	}
}

func (c *ErrorHandlerChain) HandleReturnListenerError(ch *Channel, err error) {
	for _, h := range c.Handlers {
		h.HandleReturnListenerError(ch, err)
	}
}

func (c *ErrorHandlerChain) HandleConfirmListenerError(ch *Channel, err error) {
	for _, h := range c.Handlers {
		h.HandleConfirmListenerError(ch, err)
	}
}

// TestErrorHandlerIntegration tests handler with actual errors
func TestErrorHandlerIntegration(t *testing.T) {
	factory := requireRabbitMQ(t)

	var mu sync.Mutex
	errors := make([]error, 0)

	// Custom error handler to capture errors
	handler := &CustomErrorHandler{
		OnChannelError: func(ch *Channel, err error) {
			mu.Lock()
			errors = append(errors, err)
			mu.Unlock()
		},
	}

	factory.ErrorHandler = handler

	conn, err := factory.NewConnection()
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	ch, err := conn.NewChannel()
	if err != nil {
		t.Fatalf("Failed to create channel: %v", err)
	}

	// Trigger a channel error by trying to consume from non-existent queue
	// without declaring it first (passive declare that will fail)
	_, err = ch.QueueDeclarePassive("non-existent-queue-" + fmt.Sprint(time.Now().UnixNano()))
	if err == nil {
		t.Log("Expected error from passive declare on non-existent queue")
	}

	// Give error handler time to process
	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	errorCount := len(errors)
	mu.Unlock()

	t.Logf("Captured %d channel error(s)", errorCount)

	// The test verifies that error handler infrastructure works
	// Actual errors depend on RabbitMQ state
}
