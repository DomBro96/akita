package http

import (
	"akita/logger"
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"time"
)

func WriteResponse(w http.ResponseWriter, code int, resp interface{}) {
	w.Header().Set("Content-Type", "application/json")
	data, err := json.Marshal(resp)
	if err != nil {
		logger.Errorf("Failed to encode data to JSON. Data %v Error %v.", resp, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(code)
	_, err = w.Write(data)
	if err != nil {
		logger.Errorf("Failed to write http response %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func WriteResponseWithContextType(w http.ResponseWriter, code int, contentType string, resp interface{}) {
	w.Header().Set("Content-Type", contentType)
	data, err := json.Marshal(resp)
	if err != nil {
		logger.Errorf("Failed to encode data to JSON. Data %v Error %v.", resp, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(code)
	_, err = w.Write(data)
	if err != nil {
		logger.Errorf("Failed to write http response %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

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

func (hc *HttpClient) PostForm(url string, args url.Values) (int, []byte, error) {
	resp, err := hc.client.PostForm(url, args)
	if err != nil {
		return 0, nil, err
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	return resp.StatusCode, data, err
}

func (hc *HttpClient) Post(url string, contentType string, body io.Reader) (int, []byte, error) {
	resp, err := hc.client.Post(url, contentType, body)
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
