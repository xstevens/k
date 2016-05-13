package main

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"gopkg.in/Shopify/sarama.v1"
)

func brokers() []string {
	s := os.Getenv("KAFKA_BROKERS")
	if s == "" {
		s = "127.0.0.1:9092"
	}
	return strings.Split(s, ",")
}

func offsets(client sarama.Client, topic string, partition int32) (oldest int64, newest int64) {
	oldest, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
	must(err)
	newest, err = client.GetOffset(topic, partition, sarama.OffsetNewest)
	must(err)
	return oldest, newest
}

func tlsConfig() (useTLS bool, config *tls.Config, err error) {
	// if SSL_CA_BUNDLE_PATH isn't set, just don't use TLS at all
	caPath := os.Getenv("SSL_CA_BUNDLE_PATH")
	if caPath == "" {
		return
	}
	useTLS = true
	config = new(tls.Config)
	caCerts, err := ioutil.ReadFile(caPath)
	if err != nil {
		err = fmt.Errorf("error reading $SSL_CA_BUNDLE_PATH: %v", err)
		return
	}

	config.RootCAs = x509.NewCertPool()
	if !config.RootCAs.AppendCertsFromPEM(caCerts) {
		err = fmt.Errorf("$SSL_CA_BUNDLE_PATH=%q was empty", caPath)
		return
	}

	// if $SSL_CERT_PATH or $SSL_KEY_PATH aren't set, skip client cert
	certPath, keyPath := os.Getenv("SSL_CRT_PATH"), os.Getenv("SSL_KEY_PATH")
	if certPath == "" || keyPath == "" {
		fmt.Println("SSL_CRT_PATH or SSL_KEY_PATH was empty!")
		return
	}

	keypair, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		err = fmt.Errorf("error reading $SSL_CRT_PATH/$SSL_KEY_PATH: %v", err)
		return
	}

	config.Certificates = []tls.Certificate{keypair}

	return
}
