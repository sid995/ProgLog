package config

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig selects the local certificate, trusted CA, and client/server TLS role.
type TLSConfig struct {
	CertFile string
	KeyFile  string
	CAFile   string
	// ServerAddress supplies the TLS verification name when a CA file is loaded.
	ServerAddress string
	// Server selects client-certificate verification rather than server trust roots.
	Server bool
}

// SetupTLSConfig loads optional PEM credentials and CA trust.
// A certificate is loaded only when both paths are supplied; a server with a CA
// requires verified client certificates, while a client trusts that CA for servers.
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error) {
	var err error
	tlsConfig := &tls.Config{}
	if cfg.CertFile != "" && cfg.KeyFile != "" {
		tlsConfig.Certificates = make([]tls.Certificate, 1)
		tlsConfig.Certificates[0], err = tls.LoadX509KeyPair(
			cfg.CertFile,
			cfg.KeyFile,
		)
		if err != nil {
			return nil, err
		}
	}
	if cfg.CAFile != "" {
		b, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, err
		}
		ca := x509.NewCertPool()
		ok := ca.AppendCertsFromPEM([]byte(b))
		if !ok {
			return nil, fmt.Errorf("failed to parse root certificates: %q", cfg.CAFile)
		}
		// ClientCAs validates inbound identities; RootCAs validates outbound servers.
		if cfg.Server {
			tlsConfig.ClientCAs = ca
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = ca
		}
		tlsConfig.ServerName = cfg.ServerAddress
	}
	return tlsConfig, nil
}
