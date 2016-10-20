package main

import (
	"crypto/tls"
	"fmt"
	"os"
)

var cmdTLS = &Command{
	Usage: "tls",
	Short: "connect to broker using Transport Layer Security",
	Long: `
Connects to broker using Transport Layer Security which can be useful for TLS
handshake debugging.

Example:

    $ k tls`,
	Run: runTLS,
}

func printConnectionState(connState tls.ConnectionState) {
	switch connState.Version {
	case tls.VersionSSL30:
		fmt.Printf("Version: %s\n", "SSLv3")
		break
	case tls.VersionTLS10:
		fmt.Printf("Version: %s\n", "TLSv1.0")
		break
	case tls.VersionTLS11:
		fmt.Printf("Version: %s\n", "TLSv1.1")
		break
	case tls.VersionTLS12:
		fmt.Printf("Version: %s\n", "TLSv1.2")
		break
	default:
		fmt.Printf("Version: %d\n", connState.Version)
		break
	}
	fmt.Printf("HandshakeComplete: %t\n", connState.HandshakeComplete)
	fmt.Printf("NegotiatedProtocol: %s\n", connState.NegotiatedProtocol)
	fmt.Printf("NegotiatedProtocolIsMutual: %t\n", connState.NegotiatedProtocolIsMutual)
	fmt.Printf("CipherSuite: %#x\n", connState.CipherSuite)
}

func runTLS(cmd *Command, args []string) {
	useTLS, tlsConfig, err := tlsConfig()
	must(err)
	brokers := brokers(useTLS)

	fmt.Printf("Number of Certificates: %d\n", len(tlsConfig.Certificates))

	conn, err := tls.Dial("tcp", brokers[0], tlsConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect with TCP client: %v\n", err)
		return
	}
	defer conn.Close()

	if err := conn.Handshake(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to handshake: %v\n", err)
		return
	}

	printConnectionState(conn.ConnectionState())

	fmt.Println("TLS check done.")
}

func init() {
}
