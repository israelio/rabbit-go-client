package rabbitmq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// ParseURI parses an AMQP URI and returns a ConnectionFactory configured accordingly
// Supports formats:
//   amqp://username:password@host:port/vhost
//   amqps://username:password@host:port/vhost?param=value
func ParseURI(uri string) (*ConnectionFactory, error) {
	u, err := url.Parse(uri)
	if err != nil {
		return nil, fmt.Errorf("invalid URI: %w", err)
	}

	// Validate scheme
	var useTLS bool
	switch u.Scheme {
	case "amqp":
		useTLS = false
	case "amqps":
		useTLS = true
	case "":
		return nil, errors.New("missing URI scheme (amqp:// or amqps://)")
	default:
		return nil, fmt.Errorf("unsupported URI scheme: %s (use amqp:// or amqps://)", u.Scheme)
	}

	// Extract credentials
	username := "guest"
	password := "guest"
	if u.User != nil {
		username = u.User.Username()
		if p, ok := u.User.Password(); ok {
			password = p
		}
	}

	// Extract host and port
	host := u.Hostname()
	if host == "" {
		host = "localhost"
	}

	port := 5672
	if useTLS {
		port = 5671
	}
	if u.Port() != "" {
		p, err := strconv.Atoi(u.Port())
		if err != nil {
			return nil, fmt.Errorf("invalid port: %s", u.Port())
		}
		port = p
	}

	// Extract vhost
	vhost := "/"
	if u.Path != "" && u.Path != "/" {
		vhost = strings.TrimPrefix(u.Path, "/")
		// URL decode vhost
		vhost, err = url.PathUnescape(vhost)
		if err != nil {
			return nil, fmt.Errorf("invalid vhost: %w", err)
		}
	}

	// Create factory with basic settings
	factory := &ConnectionFactory{
		Host:     host,
		Port:     port,
		VHost:    vhost,
		Username: username,
		Password: password,
	}

	// Set default timeouts
	factory.ConnectionTimeout = 60 * time.Second
	factory.HandshakeTimeout = 10 * time.Second
	factory.Heartbeat = 60 * time.Second

	// Parse query parameters
	query := u.Query()

	// Heartbeat
	if hb := query.Get("heartbeat"); hb != "" {
		seconds, err := strconv.Atoi(hb)
		if err != nil {
			return nil, fmt.Errorf("invalid heartbeat: %s", hb)
		}
		factory.Heartbeat = time.Duration(seconds) * time.Second
	}

	// Connection timeout
	if ct := query.Get("connection_timeout"); ct != "" {
		ms, err := strconv.Atoi(ct)
		if err != nil {
			return nil, fmt.Errorf("invalid connection_timeout: %s", ct)
		}
		factory.ConnectionTimeout = time.Duration(ms) * time.Millisecond
	}

	// Channel max
	if cm := query.Get("channel_max"); cm != "" {
		val, err := strconv.ParseUint(cm, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("invalid channel_max: %s", cm)
		}
		factory.ChannelMax = uint16(val)
	}

	// Frame max
	if fm := query.Get("frame_max"); fm != "" {
		val, err := strconv.ParseUint(fm, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid frame_max: %s", fm)
		}
		factory.FrameMax = uint32(val)
	}

	// TLS configuration
	if useTLS {
		tlsConfig := &tls.Config{
			ServerName: host,
		}

		// Server name indication
		if sni := query.Get("server_name_indication"); sni != "" {
			tlsConfig.ServerName = sni
		}

		// Skip verification (use with caution!)
		if verify := query.Get("verify"); verify == "false" {
			tlsConfig.InsecureSkipVerify = true
		}

		factory.TLS = tlsConfig
	}

	return factory, nil
}

// SetURI configures the ConnectionFactory using an AMQP URI
func (cf *ConnectionFactory) SetURI(uri string) error {
	parsed, err := ParseURI(uri)
	if err != nil {
		return err
	}

	// Copy parsed values to this factory
	cf.Host = parsed.Host
	cf.Port = parsed.Port
	cf.VHost = parsed.VHost
	cf.Username = parsed.Username
	cf.Password = parsed.Password
	cf.ConnectionTimeout = parsed.ConnectionTimeout
	cf.HandshakeTimeout = parsed.HandshakeTimeout
	cf.Heartbeat = parsed.Heartbeat
	cf.ChannelMax = parsed.ChannelMax
	cf.FrameMax = parsed.FrameMax
	cf.TLS = parsed.TLS

	return nil
}

// GetURI returns the AMQP URI for this connection factory
func (cf *ConnectionFactory) GetURI() string {
	scheme := "amqp"
	if cf.TLS != nil {
		scheme = "amqps"
	}

	// Build user info
	userInfo := ""
	if cf.Username != "" || cf.Password != "" {
		userInfo = url.UserPassword(cf.Username, cf.Password).String() + "@"
	}

	// URL encode vhost
	vhost := cf.VHost
	if vhost == "/" {
		vhost = ""
	} else {
		vhost = "/" + url.PathEscape(vhost)
	}

	return fmt.Sprintf("%s://%s%s:%d%s", scheme, userInfo, cf.Host, cf.Port, vhost)
}

// NewConnectionFactoryFromURI creates a new ConnectionFactory from an AMQP URI
// This is a convenience function equivalent to ParseURI
func NewConnectionFactoryFromURI(uri string) (*ConnectionFactory, error) {
	return ParseURI(uri)
}
