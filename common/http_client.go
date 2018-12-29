package common

import (
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
)

const (
	ConnectTimeout        = 500 * time.Millisecond
	KeepAlivePeriod       = 0 * time.Second // idle conn timeout only 30 second
	TLSHandshakeTimeout   = 1 * time.Second
	ExceptContinueTimeout = 500 * time.Millisecond

	// idle connection options
	MaxIdleConnections        = 2048
	MaxIdleConnectionsPerHost = MaxIdleConnections
	IdleConnTimeout           = 30 * time.Second
)

var (
	defaultTransport http.RoundTripper
)

func GetDefaultTransport() http.RoundTripper {
	if defaultTransport == nil {
		dialContext := (&net.Dialer{
			Timeout:   ConnectTimeout,
			KeepAlive: KeepAlivePeriod,
			DualStack: true,
		}).DialContext

		defaultTransport = &http.Transport{
			DialContext:           dialContext,
			TLSHandshakeTimeout:   TLSHandshakeTimeout,
			MaxIdleConns:          MaxIdleConnections,
			IdleConnTimeout:       IdleConnTimeout,
			ExpectContinueTimeout: ExceptContinueTimeout,
			MaxIdleConnsPerHost:   MaxIdleConnectionsPerHost,
		}
	}
	return defaultTransport
}


type (
	HttpClient struct {
		client http.Client
	}
)

func NewHttpClient(timeout time.Duration) *HttpClient {
	return &HttpClient{
		client: http.Client{
			Timeout:   timeout,
			Transport: GetDefaultTransport(),
		},
	}
}

func (hc *HttpClient) Post(url string, args url.Values) (int, []byte, error) {
	resp, err := hc.client.PostForm(url, args)
	if err != nil {
		return 0, nil, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func (hc *HttpClient) Get(url string) ([]byte, error) {
	resp, err := hc.client.Get(url)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	return data, err
}
