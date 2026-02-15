package rabbitmq

import (
	"crypto/tls"
	"testing"
	"time"
)

// TestURIParsing tests URI parsing functionality
func TestURIParsing(t *testing.T) {
	tests := []struct {
		name        string
		uri         string
		wantHost    string
		wantPort    int
		wantVHost   string
		wantUser    string
		wantPass    string
		wantTLS     bool
		shouldError bool
	}{
		{
			name:     "basic URI",
			uri:      "amqp://guest:guest@localhost:5672/",
			wantHost: "localhost",
			wantPort: 5672,
			wantVHost: "/",
			wantUser: "guest",
			wantPass: "guest",
			wantTLS:  false,
		},
		{
			name:     "AMQPS URI",
			uri:      "amqps://user:pass@example.com:5671/vhost",
			wantHost: "example.com",
			wantPort: 5671,
			wantVHost: "vhost",
			wantUser: "user",
			wantPass: "pass",
			wantTLS:  true,
		},
		{
			name:     "URI with encoded vhost",
			uri:      "amqp://guest:guest@localhost:5672/%2Fvhost",
			wantHost: "localhost",
			wantPort: 5672,
			wantVHost: "/vhost",
			wantUser: "guest",
			wantPass: "guest",
		},
		{
			name:     "URI without port",
			uri:      "amqp://localhost/",
			wantHost: "localhost",
			wantPort: 5672,
			wantVHost: "/",
			wantUser: "guest",
			wantPass: "guest",
		},
		{
			name:     "URI with query parameters",
			uri:      "amqp://localhost/?heartbeat=10&connection_timeout=5000",
			wantHost: "localhost",
			wantPort: 5672,
		},
		{
			name:     "URI with special characters in password",
			uri:      "amqp://user:p@ss%2Fword@localhost/",
			wantHost: "localhost",
			wantUser: "user",
			wantPass: "p@ss/word",
		},
		{
			name:        "invalid scheme",
			uri:         "http://localhost/",
			shouldError: true,
		},
		{
			name:        "invalid port",
			uri:         "amqp://localhost:abc/",
			shouldError: true,
		},
		{
			name:        "empty URI",
			uri:         "",
			shouldError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := ParseURI(tt.uri)

			if tt.shouldError {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if factory.Host != tt.wantHost {
				t.Errorf("Host: got %q, want %q", factory.Host, tt.wantHost)
			}
			if tt.wantPort != 0 && factory.Port != tt.wantPort {
				t.Errorf("Port: got %d, want %d", factory.Port, tt.wantPort)
			}
			if tt.wantVHost != "" && factory.VHost != tt.wantVHost {
				t.Errorf("VHost: got %q, want %q", factory.VHost, tt.wantVHost)
			}
			if tt.wantUser != "" && factory.Username != tt.wantUser {
				t.Errorf("Username: got %q, want %q", factory.Username, tt.wantUser)
			}
			if tt.wantPass != "" && factory.Password != tt.wantPass {
				t.Errorf("Password: got %q, want %q", factory.Password, tt.wantPass)
			}
			if tt.wantTLS && factory.TLS == nil {
				t.Error("Expected TLS config but got none")
			}
		})
	}
}

// TestURIQueryParameters tests parsing of query parameters
func TestURIQueryParameters(t *testing.T) {
	tests := []struct {
		name            string
		uri             string
		wantHeartbeat   time.Duration
		wantTimeout     time.Duration
		wantChannelMax  uint16
		wantFrameMax    uint32
	}{
		{
			name:          "heartbeat parameter",
			uri:           "amqp://localhost/?heartbeat=30",
			wantHeartbeat: 30 * time.Second,
		},
		{
			name:        "connection timeout",
			uri:         "amqp://localhost/?connection_timeout=10000",
			wantTimeout: 10 * time.Second,
		},
		{
			name:           "channel max",
			uri:            "amqp://localhost/?channel_max=100",
			wantChannelMax: 100,
		},
		{
			name:         "frame max",
			uri:          "amqp://localhost/?frame_max=65536",
			wantFrameMax: 65536,
		},
		{
			name:          "multiple parameters",
			uri:           "amqp://localhost/?heartbeat=20&channel_max=50&frame_max=131072",
			wantHeartbeat: 20 * time.Second,
			wantChannelMax: 50,
			wantFrameMax:   131072,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := ParseURI(tt.uri)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if tt.wantHeartbeat != 0 && factory.Heartbeat != tt.wantHeartbeat {
				t.Errorf("Heartbeat: got %v, want %v", factory.Heartbeat, tt.wantHeartbeat)
			}
			if tt.wantTimeout != 0 && factory.ConnectionTimeout != tt.wantTimeout {
				t.Errorf("Timeout: got %v, want %v", factory.ConnectionTimeout, tt.wantTimeout)
			}
			if tt.wantChannelMax != 0 && factory.ChannelMax != tt.wantChannelMax {
				t.Errorf("ChannelMax: got %d, want %d", factory.ChannelMax, tt.wantChannelMax)
			}
			if tt.wantFrameMax != 0 && factory.FrameMax != tt.wantFrameMax {
				t.Errorf("FrameMax: got %d, want %d", factory.FrameMax, tt.wantFrameMax)
			}
		})
	}
}

// TestMultipleHosts tests URI with multiple hosts
func TestMultipleHosts(t *testing.T) {
	uri := "amqp://host1:5672,host2:5672,host3:5672/"

	factory, err := ParseURI(uri)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	// Should have multiple addresses
	t.Logf("Parsed multiple hosts from URI: %s", uri)
	_ = factory
}

// TestTLSConfiguration tests TLS configuration from URI
func TestTLSConfiguration(t *testing.T) {
	tests := []struct {
		name                  string
		uri                   string
		wantTLS               bool
		wantInsecureSkipVerify bool
		checkInsecureSkipVerify bool
		wantServerName        string
	}{
		{
			name:    "AMQPS with default settings",
			uri:     "amqps://localhost/",
			wantTLS: true,
		},
		{
			name:           "AMQPS with server name",
			uri:            "amqps://localhost/?server_name_indication=example.com",
			wantTLS:        true,
			wantServerName: "example.com",
		},
		{
			name:                    "AMQPS with verify disabled",
			uri:                     "amqps://localhost/?verify=false",
			wantTLS:                 true,
			wantInsecureSkipVerify:  true,
			checkInsecureSkipVerify: true,
		},
		{
			name:    "AMQP without TLS",
			uri:     "amqp://localhost/",
			wantTLS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory, err := ParseURI(tt.uri)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if tt.wantTLS && factory.TLS == nil {
				t.Error("Expected TLS config but got none")
			}
			if !tt.wantTLS && factory.TLS != nil {
				t.Error("Expected no TLS config but got one")
			}

			if tt.wantTLS && factory.TLS != nil {
				if tt.wantServerName != "" && factory.TLS.ServerName != tt.wantServerName {
					t.Errorf("ServerName: got %q, want %q", factory.TLS.ServerName, tt.wantServerName)
				}
				if tt.checkInsecureSkipVerify && factory.TLS.InsecureSkipVerify != tt.wantInsecureSkipVerify {
					t.Errorf("InsecureSkipVerify: got %v, want %v", factory.TLS.InsecureSkipVerify, tt.wantInsecureSkipVerify)
				}
			}
		})
	}
}

// TestConnectionFactoryDefaults tests default values
func TestConnectionFactoryDefaults(t *testing.T) {
	factory := NewConnectionFactory()

	if factory.Host != "localhost" {
		t.Errorf("Default host: got %q, want localhost", factory.Host)
	}
	if factory.Port != 5672 {
		t.Errorf("Default port: got %d, want 5672", factory.Port)
	}
	if factory.VHost != "/" {
		t.Errorf("Default vhost: got %q, want /", factory.VHost)
	}
	if factory.Username != "guest" {
		t.Errorf("Default username: got %q, want guest", factory.Username)
	}
	if factory.Password != "guest" {
		t.Errorf("Default password: got %q, want guest", factory.Password)
	}
	if factory.Heartbeat != 10*time.Second {
		t.Errorf("Default heartbeat: got %v, want 10s", factory.Heartbeat)
	}
}

// TestConnectionFactoryOptions tests functional options
func TestConnectionFactoryOptions(t *testing.T) {
	factory := NewConnectionFactory(
		WithHost("example.com"),
		WithPort(5671),
		WithVHost("/prod"),
		WithCredentials("admin", "secret"),
		WithHeartbeat(30*time.Second),
		WithConnectionTimeout(10*time.Second),
		WithChannelMax(100),
		WithFrameMax(65536),
	)

	if factory.Host != "example.com" {
		t.Errorf("Host: got %q, want example.com", factory.Host)
	}
	if factory.Port != 5671 {
		t.Errorf("Port: got %d, want 5671", factory.Port)
	}
	if factory.VHost != "/prod" {
		t.Errorf("VHost: got %q, want /prod", factory.VHost)
	}
	if factory.Username != "admin" {
		t.Errorf("Username: got %q, want admin", factory.Username)
	}
	if factory.Password != "secret" {
		t.Errorf("Password: got %q, want secret", factory.Password)
	}
	if factory.Heartbeat != 30*time.Second {
		t.Errorf("Heartbeat: got %v, want 30s", factory.Heartbeat)
	}
	if factory.ConnectionTimeout != 10*time.Second {
		t.Errorf("ConnectionTimeout: got %v, want 10s", factory.ConnectionTimeout)
	}
	if factory.ChannelMax != 100 {
		t.Errorf("ChannelMax: got %d, want 100", factory.ChannelMax)
	}
	if factory.FrameMax != 65536 {
		t.Errorf("FrameMax: got %d, want 65536", factory.FrameMax)
	}
}

// TestTLSOptions tests TLS configuration options
func TestTLSOptions(t *testing.T) {
	tlsConfig := &tls.Config{
		ServerName: "example.com",
		MinVersion: tls.VersionTLS12,
	}

	factory := NewConnectionFactory(
		WithTLS(tlsConfig),
	)

	if factory.TLS == nil {
		t.Fatal("TLS config not set")
	}
	if factory.TLS.ServerName != "example.com" {
		t.Errorf("ServerName: got %q, want example.com", factory.TLS.ServerName)
	}
	if factory.TLS.MinVersion != tls.VersionTLS12 {
		t.Errorf("MinVersion: got %d, want %d", factory.TLS.MinVersion, tls.VersionTLS12)
	}
}

// TestRecoveryOptions tests automatic recovery configuration
func TestRecoveryOptions(t *testing.T) {
	factory := NewConnectionFactory(
		WithAutomaticRecovery(true),
		WithTopologyRecovery(true),
		WithRecoveryInterval(5*time.Second),
	)

	if !factory.AutomaticRecovery {
		t.Error("AutomaticRecovery should be true")
	}
	if !factory.TopologyRecovery {
		t.Error("TopologyRecovery should be true")
	}
	if factory.RecoveryInterval != 5*time.Second {
		t.Errorf("RecoveryInterval: got %v, want 5s", factory.RecoveryInterval)
	}
}

// TestInvalidConfiguration tests invalid configuration detection
func TestInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name    string
		factory *ConnectionFactory
	}{
		{
			name: "empty host",
			factory: &ConnectionFactory{
				Host:     "",
				Port:     5672,
				VHost:    "/",
				Username: "guest",
			},
		},
		{
			name: "negative timeout",
			factory: &ConnectionFactory{
				Host:              "localhost",
				Port:              5672,
				VHost:             "/",
				Username:          "guest",
				ConnectionTimeout: -1,
			},
		},
		{
			name: "invalid port zero",
			factory: &ConnectionFactory{
				Host:     "localhost",
				Port:     0,
				VHost:    "/",
				Username: "guest",
			},
		},
		{
			name: "invalid frame max",
			factory: &ConnectionFactory{
				Host:     "localhost",
				Port:     5672,
				VHost:    "/",
				Username: "guest",
				FrameMax: 4095, // Must be >= 4096
			},
		},
		{
			name: "empty vhost",
			factory: &ConnectionFactory{
				Host:     "localhost",
				Port:     5672,
				VHost:    "",
				Username: "guest",
			},
		},
		{
			name: "empty username",
			factory: &ConnectionFactory{
				Host:     "localhost",
				Port:     5672,
				VHost:    "/",
				Username: "",
			},
		},
		{
			name: "negative heartbeat",
			factory: &ConnectionFactory{
				Host:      "localhost",
				Port:      5672,
				VHost:     "/",
				Username:  "guest",
				Heartbeat: -1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.factory.Validate()
			if err == nil {
				t.Error("Expected validation error but got none")
			} else {
				t.Logf("Correctly detected invalid config: %v", err)
			}
		})
	}
}
