package protocol

// AMQP protocol version
const (
	ProtocolVersionMajor = 0
	ProtocolVersionMinor = 9
	ProtocolVersionRevision = 1

	ProtocolHeader = "AMQP\x00\x00\x09\x01"
)

// Frame types
const (
	FrameMethod    = 1
	FrameHeader    = 2
	FrameBody      = 3
	FrameHeartbeat = 8
	FrameEnd       = 0xCE // Frame terminator byte
)

// AMQP Class IDs
const (
	ClassConnection = 10
	ClassChannel    = 20
	ClassExchange   = 40
	ClassQueue      = 50
	ClassBasic      = 60
	ClassTx         = 90
	ClassConfirm    = 85
)

// Connection method IDs
const (
	MethodConnectionStart     = 10
	MethodConnectionStartOk   = 11
	MethodConnectionSecure    = 20
	MethodConnectionSecureOk  = 21
	MethodConnectionTune      = 30
	MethodConnectionTuneOk    = 31
	MethodConnectionOpen      = 40
	MethodConnectionOpenOk    = 41
	MethodConnectionClose     = 50
	MethodConnectionCloseOk   = 51
	MethodConnectionBlocked   = 60
	MethodConnectionUnblocked = 61
)

// Channel method IDs
const (
	MethodChannelOpen    = 10
	MethodChannelOpenOk  = 11
	MethodChannelFlow    = 20
	MethodChannelFlowOk  = 21
	MethodChannelClose   = 40
	MethodChannelCloseOk = 41
)

// Exchange method IDs
const (
	MethodExchangeDeclare      = 10
	MethodExchangeDeclareOk    = 11
	MethodExchangeDelete       = 20
	MethodExchangeDeleteOk     = 21
	MethodExchangeBind         = 30
	MethodExchangeBindOk       = 31
	MethodExchangeUnbind       = 40
	MethodExchangeUnbindOk     = 51
)

// Queue method IDs
const (
	MethodQueueDeclare    = 10
	MethodQueueDeclareOk  = 11
	MethodQueueBind       = 20
	MethodQueueBindOk     = 21
	MethodQueuePurge      = 30
	MethodQueuePurgeOk    = 31
	MethodQueueDelete     = 40
	MethodQueueDeleteOk   = 41
	MethodQueueUnbind     = 50
	MethodQueueUnbindOk   = 51
)

// Basic method IDs
const (
	MethodBasicQos       = 10
	MethodBasicQosOk     = 11
	MethodBasicConsume   = 20
	MethodBasicConsumeOk = 21
	MethodBasicCancel    = 30
	MethodBasicCancelOk  = 31
	MethodBasicPublish   = 40
	MethodBasicReturn    = 50
	MethodBasicDeliver   = 60
	MethodBasicGet       = 70
	MethodBasicGetOk     = 71
	MethodBasicGetEmpty  = 72
	MethodBasicAck       = 80
	MethodBasicReject    = 90
	MethodBasicRecoverAsync = 100
	MethodBasicRecover   = 110
	MethodBasicRecoverOk = 111
	MethodBasicNack      = 120
)

// Tx method IDs
const (
	MethodTxSelect     = 10
	MethodTxSelectOk   = 11
	MethodTxCommit     = 20
	MethodTxCommitOk   = 21
	MethodTxRollback   = 30
	MethodTxRollbackOk = 31
)

// Confirm method IDs
const (
	MethodConfirmSelect   = 10
	MethodConfirmSelectOk = 11
)

// AMQP reply codes
const (
	ReplySuccess                 = 200
	ReplyContentTooLarge        = 311
	ReplyNoRoute                = 312
	ReplyNoConsumers            = 313
	ReplyConnectionForced       = 320
	ReplyInvalidPath            = 402
	ReplyAccessRefused          = 403
	ReplyNotFound               = 404
	ReplyResourceLocked         = 405
	ReplyPreconditionFailed     = 406
	ReplyFrameError             = 501
	ReplySyntaxError            = 502
	ReplyCommandInvalid         = 503
	ReplyChannelError           = 504
	ReplyUnexpectedFrame        = 505
	ReplyResourceError          = 506
	ReplyNotAllowed             = 530
	ReplyNotImplemented         = 540
	ReplyInternalError          = 541
)

// Built-in exchange types
const (
	ExchangeTypeDirect  = "direct"
	ExchangeTypeFanout  = "fanout"
	ExchangeTypeTopic   = "topic"
	ExchangeTypeHeaders = "headers"
)

// Default exchange name
const (
	DefaultExchange = ""
)

// Delivery modes
const (
	DeliveryModeNonPersistent = 1
	DeliveryModePersistent    = 2
)

// Frame size constants
const (
	FrameMinSize     = 4096
	FrameHeaderSize  = 7  // Frame type (1) + Channel ID (2) + Size (4)
	FrameEndSize     = 1  // Frame end marker
)
