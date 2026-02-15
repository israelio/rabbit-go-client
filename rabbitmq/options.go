package rabbitmq

import (
	"crypto/tls"
	"time"
)

// FactoryOption is a functional option for ConnectionFactory
type FactoryOption func(*ConnectionFactory)

// WithHost sets the host to connect to
func WithHost(host string) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.Host = host
	}
}

// WithPort sets the port to connect to
func WithPort(port int) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.Port = port
	}
}

// WithCredentials sets the username and password
func WithCredentials(username, password string) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.Username = username
		cf.Password = password
	}
}

// WithVHost sets the virtual host
func WithVHost(vhost string) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.VHost = vhost
	}
}

// WithTLS enables TLS with the given configuration
func WithTLS(config *tls.Config) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.TLS = config
	}
}

// WithConnectionTimeout sets the connection timeout
func WithConnectionTimeout(timeout time.Duration) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.ConnectionTimeout = timeout
	}
}

// WithHandshakeTimeout sets the handshake timeout
func WithHandshakeTimeout(timeout time.Duration) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.HandshakeTimeout = timeout
	}
}

// WithHeartbeat sets the heartbeat interval
func WithHeartbeat(interval time.Duration) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.Heartbeat = interval
	}
}

// WithChannelMax sets the maximum number of channels
func WithChannelMax(max uint16) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.ChannelMax = max
	}
}

// WithFrameMax sets the maximum frame size
func WithFrameMax(max uint32) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.FrameMax = max
	}
}

// WithAutomaticRecovery enables or disables automatic recovery
func WithAutomaticRecovery(enabled bool) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.AutomaticRecovery = enabled
	}
}

// WithTopologyRecovery enables or disables topology recovery
func WithTopologyRecovery(enabled bool) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.TopologyRecovery = enabled
	}
}

// WithRecoveryInterval sets the interval between recovery attempts
func WithRecoveryInterval(interval time.Duration) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.RecoveryInterval = interval
	}
}

// WithConnectionRetryAttempts sets the number of connection retry attempts
func WithConnectionRetryAttempts(attempts int) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.ConnectionRetryAttempts = attempts
	}
}

// WithClientProperties sets custom client properties
func WithClientProperties(properties map[string]interface{}) FactoryOption {
	return func(cf *ConnectionFactory) {
		if cf.ClientProperties == nil {
			cf.ClientProperties = make(map[string]interface{})
		}
		for k, v := range properties {
			cf.ClientProperties[k] = v
		}
	}
}

// WithClientProperty sets a single client property
func WithClientProperty(key string, value interface{}) FactoryOption {
	return func(cf *ConnectionFactory) {
		if cf.ClientProperties == nil {
			cf.ClientProperties = make(map[string]interface{})
		}
		cf.ClientProperties[key] = value
	}
}

// WithErrorHandler sets a custom error handler
func WithErrorHandler(handler ErrorHandler) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.ErrorHandler = handler
	}
}

// WithRecoveryHandler sets a custom recovery handler
func WithRecoveryHandler(handler RecoveryHandler) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.RecoveryHandler = handler
	}
}

// WithBlockedHandler sets a custom blocked connection handler
func WithBlockedHandler(handler BlockedHandler) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.BlockedHandler = handler
	}
}

// WithLogger sets a custom logger
func WithLogger(logger Logger) FactoryOption {
	return func(cf *ConnectionFactory) {
		cf.Logger = logger
		if cf.ErrorHandler == nil {
			cf.ErrorHandler = &DefaultErrorHandler{Logger: logger}
		}
	}
}
