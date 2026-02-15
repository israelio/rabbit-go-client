package rabbitmq

import (
	"fmt"

	"github.com/israelio/rabbit-go-client/internal/protocol"
)

// Error represents an AMQP error
type Error struct {
	Code    int
	Reason  string
	Server  bool   // true if error originated from server
	Recover bool   // true if connection/channel can be recovered
}

// Error implements the error interface
func (e *Error) Error() string {
	origin := "client"
	if e.Server {
		origin = "server"
	}
	return fmt.Sprintf("AMQP error %d (%s): %s", e.Code, origin, e.Reason)
}

// Predefined errors matching AMQP reply codes
var (
	ErrClosed = &Error{
		Code:    protocol.ReplyConnectionForced,
		Reason:  "connection closed",
		Server:  false,
		Recover: false,
	}

	ErrChannelClosed = &Error{
		Code:    protocol.ReplyChannelError,
		Reason:  "channel closed",
		Server:  false,
		Recover: false,
	}

	ErrNotFound = &Error{
		Code:    protocol.ReplyNotFound,
		Reason:  "resource not found",
		Server:  true,
		Recover: false,
	}

	ErrAccessRefused = &Error{
		Code:    protocol.ReplyAccessRefused,
		Reason:  "access refused",
		Server:  true,
		Recover: false,
	}

	ErrPreconditionFailed = &Error{
		Code:    protocol.ReplyPreconditionFailed,
		Reason:  "precondition failed",
		Server:  true,
		Recover: false,
	}

	ErrResourceLocked = &Error{
		Code:    protocol.ReplyResourceLocked,
		Reason:  "resource locked",
		Server:  true,
		Recover: false,
	}

	ErrFrameError = &Error{
		Code:    protocol.ReplyFrameError,
		Reason:  "frame error",
		Server:  false,
		Recover: false,
	}

	ErrSyntaxError = &Error{
		Code:    protocol.ReplySyntaxError,
		Reason:  "syntax error",
		Server:  true,
		Recover: false,
	}

	ErrCommandInvalid = &Error{
		Code:    protocol.ReplyCommandInvalid,
		Reason:  "command invalid",
		Server:  true,
		Recover: false,
	}

	ErrChannelError = &Error{
		Code:    protocol.ReplyChannelError,
		Reason:  "channel error",
		Server:  true,
		Recover: false,
	}

	ErrUnexpectedFrame = &Error{
		Code:    protocol.ReplyUnexpectedFrame,
		Reason:  "unexpected frame",
		Server:  true,
		Recover: false,
	}

	ErrResourceError = &Error{
		Code:    protocol.ReplyResourceError,
		Reason:  "resource error",
		Server:  true,
		Recover: false,
	}

	ErrNotAllowed = &Error{
		Code:    protocol.ReplyNotAllowed,
		Reason:  "not allowed",
		Server:  true,
		Recover: false,
	}

	ErrNotImplemented = &Error{
		Code:    protocol.ReplyNotImplemented,
		Reason:  "not implemented",
		Server:  true,
		Recover: false,
	}

	ErrInternalError = &Error{
		Code:    protocol.ReplyInternalError,
		Reason:  "internal error",
		Server:  true,
		Recover: false,
	}

	ErrContentTooLarge = &Error{
		Code:    protocol.ReplyContentTooLarge,
		Reason:  "content too large",
		Server:  true,
		Recover: false,
	}

	ErrNoRoute = &Error{
		Code:    protocol.ReplyNoRoute,
		Reason:  "no route",
		Server:  true,
		Recover: false,
	}

	ErrNoConsumers = &Error{
		Code:    protocol.ReplyNoConsumers,
		Reason:  "no consumers",
		Server:  true,
		Recover: false,
	}
)

// NewError creates a new Error from reply code and text
func NewError(code int, reason string, server bool) *Error {
	return &Error{
		Code:    code,
		Reason:  reason,
		Server:  server,
		Recover: code != protocol.ReplyConnectionForced && code < 500,
	}
}

// ErrorHandler handles connection and channel errors
type ErrorHandler interface {
	HandleConnectionError(conn *Connection, err error)
	HandleChannelError(ch *Channel, err error)
	HandleConsumerError(ch *Channel, consumerTag string, err error)
	HandleReturnListenerError(ch *Channel, err error)
	HandleConfirmListenerError(ch *Channel, err error)
}

// DefaultErrorHandler provides default error handling with logging
type DefaultErrorHandler struct {
	Logger Logger
}

// HandleConnectionError logs connection errors
func (deh *DefaultErrorHandler) HandleConnectionError(conn *Connection, err error) {
	if deh.Logger != nil {
		deh.Logger.Printf("Connection error: %v", err)
	}
}

// HandleChannelError logs channel errors
func (deh *DefaultErrorHandler) HandleChannelError(ch *Channel, err error) {
	if deh.Logger != nil {
		deh.Logger.Printf("Channel %d error: %v", ch.id, err)
	}
}

// HandleConsumerError logs consumer errors
func (deh *DefaultErrorHandler) HandleConsumerError(ch *Channel, consumerTag string, err error) {
	if deh.Logger != nil {
		deh.Logger.Printf("Consumer %s error: %v", consumerTag, err)
	}
}

// HandleReturnListenerError logs return listener errors
func (deh *DefaultErrorHandler) HandleReturnListenerError(ch *Channel, err error) {
	if deh.Logger != nil {
		deh.Logger.Printf("Return listener error: %v", err)
	}
}

// HandleConfirmListenerError logs confirm listener errors
func (deh *DefaultErrorHandler) HandleConfirmListenerError(ch *Channel, err error) {
	if deh.Logger != nil {
		deh.Logger.Printf("Confirm listener error: %v", err)
	}
}

// Logger interface for custom logging
type Logger interface {
	Printf(format string, v ...interface{})
	Println(v ...interface{})
}
