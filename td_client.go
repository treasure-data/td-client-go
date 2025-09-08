//
// Treasure Data API client for Go
//
// Copyright (C) 2014 Treasure Data, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

package td_client

import (
	"bytes"
	"crypto/md5"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ugorji/go/codec"
)

const (
	GenericError = iota
	AuthError
	ForbiddenError
	AlreadyExistsError
	NotFoundError
)

const (
	DEFAULT_ENDPOINT            = "api.treasure-data.com"
	DEFAULT_IMPORT_ENDPOINT     = "api-import.treasure-data.com"
	NEW_DEFAULT_ENDPOINT        = "api.treasuredata.com"
	NEW_DEFAULT_IMPORT_ENDPOINT = "api-import.treasuredata.com"
)

const (
	CLIENT_VERSION = "0.4.0"
)

const (
	// Represents the date/time format for time.Time.Format(),
	// which is used in several API function parameters and results.
	TDAPIDateTime            = "2006-01-02 15:04:05 MST"
	TDAPIDateTimeNumericZone = "2006-01-02 15:04:05 -0700"
)

// APIError represents an error that has occurred during the API call.
type APIError struct {
	Type    int
	Message string
	Cause   error
}

func stringizeAPIErrorType(type_ int) string {
	switch type_ {
	case GenericError:
		return "GenericError"
	case AuthError:
		return "AuthError"
	case ForbiddenError:
		return "ForbiddenError"
	case AlreadyExistsError:
		return "AlreadyExistsError"
	case NotFoundError:
		return "NotFoundError"
	}
	return "Unknown"
}

func (e *APIError) Error() string {
	retval := fmt.Sprintf("%s: %s", stringizeAPIErrorType(e.Type), e.Message)
	if e.Cause != nil {
		retval += fmt.Sprintf(" (cause: %s)", e.Cause.Error())
	}
	return retval
}

// EndpointRouter is expected to return the host name most suitable for the passed request URI
type EndpointRouter interface {
	Route(requestUri string) string
}

// Settings stores the parameters for initializaing TDClient.
//
// Note that ReadTimeout / SendTimeout includes the time taken for receiving / sending the actual data in addition to the idle time, so it is advised to set the value long enough depending on the circumstances. (network latency etc.)
//
// Specifying 0 to Port means the value will be automatically determined according to the settings.
//
// Proxy can take three kinds of values: *url.URL (parsed URL), func(*http.Request)(*url.URL, error), string (URL) or nil (the direct connection to the endpoint is possible).
//
// Transport allows you to take more control over the communication.
//
// `Ssl` option was removed from client options.
// td-client-go no longer support `Ssl` option since Treasure Data permits only HTTPS access after September 1, 2020.
type Settings struct {
	ApiKey            string            // Treasure Data Account API key
	UserAgent         string            // (Optional) Name that will appear as the User-Agent HTTP header
	Router            EndpointRouter    // (Optional) Endpoint router
	ConnectionTimeout time.Duration     // (Optional) Connection timeout
	ReadTimeout       time.Duration     // (Optional) Read timeout.
	SendTimeout       time.Duration     // (Optional) Send timeout.
	RootCAs           *x509.CertPool    // (Optional) Specify the CA certificates.
	Port              int               // (Optional) Port number.
	Proxy             interface{}       // (Optional) HTTP proxy to use.
	Transport         http.RoundTripper // (Optional) Overrides the transport used to establish the connection.
	Headers           map[string]string // (Optional) Additional headers that will be sent to the endpoint.
}

// A FixedEndpointRouter instance represents an EndpointRouter that always routes the request to the same endpoint.
type FixedEndpointRouter struct {
	Endpoint string
}

func (r *FixedEndpointRouter) Route(_ string) string {
	return r.Endpoint
}

// V3EndpointRouter routes the import request to the dedicated endpoint and other requests to the default.
type V3EndpointRouter struct {
	DefaultEndpoint string
	ImportEndpoint  string
}

func (r *V3EndpointRouter) Route(requestUri string) string {
	if strings.HasPrefix(requestUri, "/v3/table/import/") || strings.HasPrefix(requestUri, "/v3/table/import_with_id/") {
		return r.ImportEndpoint
	} else {
		return r.DefaultEndpoint
	}
}

// DefaultRouter is a V3EndpointRouter with the hard-coded endpoints.
var DefaultRouter = V3EndpointRouter{
	DefaultEndpoint: NEW_DEFAULT_ENDPOINT,
	ImportEndpoint:  NEW_DEFAULT_IMPORT_ENDPOINT,
}

// TDClient represents a context used to talk to the Treasure Data API.
type TDClient struct {
	apiKey            string
	userAgent         string
	router            EndpointRouter
	ssl               bool // nolint:unused
	rootCAs           *x509.CertPool
	port              int
	connectionTimeout time.Duration
	readTimeout       time.Duration
	sendTimeout       time.Duration
	transport         http.RoundTripper
	headers           map[string]string
	mpCodec           *codec.MsgpackHandle
}

// Blob denotes a concept, which is opaque data that can be read bytewise through an io.Reader, has a certain size and provides a calculated MD5 sum.
type Blob interface {
	Reader() (io.ReadCloser, error)
	Size() (int64, error)
	MD5Sum() ([]byte, error)
}

// Used in internal schema, marking the field as optional as well as providing the default.
type Optional struct {
	V       interface{}
	Default interface{}
}

// Used in internal schema, marking the field so that it will be unmarshaled by the specified function.
type ConverterFunc func(string) (interface{}, error)

var timeType = reflect.TypeOf(time.Time{})
var optionalType = reflect.TypeOf(Optional{})

// InMemoryBlob is a Blob which stores the entire data as a byte array.
type InMemoryBlob []byte

func (b InMemoryBlob) Reader() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(b)), nil
}

func (b InMemoryBlob) Size() (int64, error) {
	return int64(len(b)), nil
}

func (b InMemoryBlob) MD5Sum() ([]byte, error) {
	h := md5.New()
	h.Write(b)
	retval := make([]byte, 0, h.Size())
	return h.Sum(retval), nil
}

// EmbeddedJSON is a factory used internally that makes a ConverterFunc function that returns the specified type.
func EmbeddedJSON(expectedTypeProto interface{}) ConverterFunc {
	expectedType := reflect.TypeOf(expectedTypeProto)
	return func(jsStr string) (interface{}, error) {
		var retval interface{}
		switch expectedType.Kind() {
		case reflect.Map:
			retval = reflect.MakeMap(expectedType).Interface()
		case reflect.Slice:
			retval = reflect.MakeSlice(expectedType, 0, 0).Interface()
		default:
			return nil, fmt.Errorf("Unexpected prototype: %s", expectedType.String())
		}
		err := json.Unmarshal([]byte(jsStr), &retval)
		if err != nil {
			return nil, err
		}
		return retval, nil
	}
}

func (client *TDClient) buildUrl(requestUri string, params url.Values) *url.URL {
	endpoint := client.router.Route(requestUri)
	scheme := "https"
	host := endpoint
	if client.port != 0 {
		host = host + ":" + strconv.Itoa(client.port)
	}
	return &url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     requestUri,
		RawQuery: params.Encode(),
	}
}

func (client *TDClient) newRequest(method string, requestUri string, params url.Values, body Blob) (*http.Request, error) {
	getParams := (url.Values)(nil)
	contentType := "application/octet-stream"
	if method == "POST" {
		body = InMemoryBlob(params.Encode())
		contentType = "application/x-www-form-urlencoded"
	} else {
		getParams = params
	}
	err := (error)(nil)
	contentLength := int64(0)
	reader := (io.ReadCloser)(nil)
	if body != nil {
		contentLength, err = body.Size()
		if err != nil {
			return nil, err
		}
		reader, err = body.Reader()
		if err != nil {
			return nil, err
		}
	}
	url := client.buildUrl(requestUri, getParams).String()
	req, err := http.NewRequest(
		method,
		url,
		reader,
	)
	if err != nil {
		if reader != nil {
			reader.Close()
		}
		return nil, err
	}
	if body != nil {
		req.ContentLength = contentLength
	}
	for k, v := range client.headers {
		req.Header.Set(k, v)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Date", time.Now().Format(time.RFC822))
	req.Header.Set("User-Agent", client.userAgent)
	req.Header.Set("Authorization", "TD1 "+client.apiKey)
	return req, nil
}

func (client *TDClient) get(requestUri string, params url.Values) (*http.Response, error) {
	req, err := client.newRequest("GET", requestUri, params, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (client *TDClient) post(requestUri string, params url.Values) (*http.Response, error) {
	req, err := client.newRequest("POST", requestUri, params, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (client *TDClient) put(requestUri string, stream Blob) (*http.Response, error) {
	req, err := client.newRequest("PUT", requestUri, nil, stream)
	if err != nil {
		return nil, err
	}
	resp, err := client.transport.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (client *TDClient) buildError(resp *http.Response, type_ int, message string, cause error) error {
	statusCode := resp.StatusCode
	errorMessage := ""
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	js := make(map[string]interface{})
	err = json.Unmarshal(body, &js)
	if err == nil {
		m := ""
		if js == nil {
			m = resp.Status[4:]
		} else {
			var _m interface{}
			_m, ok := js["errorMessage"]
			if !ok {
				_m = js["error"]
			}
			if _m != nil {
				m, _ = _m.(string)
			}
		}
		errorMessage = m
	} else {
		errorMessage = string(body)
	}
	if type_ < 0 {
		switch statusCode {
		case 404:
			type_ = NotFoundError
			message = fmt.Sprintf("%s: %s", message, errorMessage)
		case 409:
			type_ = AlreadyExistsError
			message = fmt.Sprintf("%s: %s", message, errorMessage)
		case 401:
			type_ = AuthError
			message = fmt.Sprintf("%s: %s", message, errorMessage)
		case 403:
			type_ = ForbiddenError
			message = fmt.Sprintf("%s: %s", message, errorMessage)
		default:
			type_ = GenericError
			message = fmt.Sprintf("%d: %s: %s", statusCode, message, errorMessage)
		}
	} else {
		message = fmt.Sprintf("%d: %s: %s", statusCode, message, errorMessage)
	}
	return &APIError{
		Type:    type_,
		Message: message,
		Cause:   cause,
	}
}

func stringizeType(type_ reflect.Type) string {
	switch type_.Kind() {
	case reflect.Bool:
		return "boolean"
	case reflect.String:
		return "string"
	case reflect.Int, reflect.Int32, reflect.Int64:
		return "int"
	case reflect.Float64:
		return "float"
	case reflect.Array, reflect.Slice:
		return "[]"
	case reflect.Map:
		return "{}"
	default:
		return "(unsupported type " + type_.String() + ")"
	}
}

func integralType(type_ reflect.Type) bool {
	switch type_.Kind() {
	case reflect.Int, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

func defaultFor(type_ reflect.Type) interface{} {
	switch type_.Kind() {
	case reflect.Bool:
		return false
	case reflect.String:
		return ""
	case reflect.Int, reflect.Int32, reflect.Int64:
		return 0
	case reflect.Float32, reflect.Float64:
		return 0.
	case reflect.Slice, reflect.Array:
		return []interface{}{}
	case reflect.Map:
		return map[string]interface{}{}
	default:
		return nil
	}
}

func (client *TDClient) validateAndCoerceInner(path string, v interface{}, ev reflect.Value) (interface{}, error) {
	gottenType := (reflect.Type)(nil)
	if v != nil {
		gottenType = reflect.TypeOf(v)
	}
	if ev.Type().Kind() == reflect.Interface {
		// XXX: is this really good?
		ev = ev.Elem()
	}
	expectedType := ev.Type()
	expectedJsonType := expectedType
	optional := false
	defaultValue := (interface{})(nil)
	if expectedType == optionalType {
		optional = true
		_ev := ev.Interface().(Optional)
		ev = reflect.ValueOf(_ev.V)
		defaultValue = _ev.Default
		expectedType = ev.Type()
		expectedJsonType = expectedType
	}
	if expectedType.Kind() == reflect.Struct || expectedType.Kind() == reflect.Func {
		expectedJsonType = reflect.TypeOf("")
	}
	if gottenType == nil {
		if optional {
			if defaultValue == nil {
				defaultValue = defaultFor(expectedType)
			}
			return defaultValue, nil
		}
		return nil, fmt.Errorf("%s may not be null", path)
	}
	if expectedJsonType.Kind() != gottenType.Kind() {
		if gottenType.Kind() == reflect.Float64 && integralType(expectedJsonType) {
			v = reflect.ValueOf(v).Convert(expectedJsonType).Interface()
		} else if gottenType.Kind() == reflect.Map && expectedJsonType.Kind() == reflect.String {
			jsonString, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("%s is failed parse map to string %s", path, err.Error())
			}
			v = string(jsonString)
		} else {
			return nil, fmt.Errorf("type mismatch (%s != %s) for %s", stringizeType(gottenType), stringizeType(expectedJsonType), path)
		}
	}
	switch expectedType.Kind() {
	case reflect.Func:
		res := ev.Call([]reflect.Value{reflect.ValueOf(v)})
		v = res[0].Interface()
		err := res[1].Interface()
		// if !ok {
		// 	return nil, errors.New(fmt.Sprintf("unsupported type %s in the schema for %s", expectedType.String(), path))
		// }
		// v, err = _ev(v.(string))
		if err != nil {
			return nil, err.(error)
		}
	case reflect.Struct:
		if expectedType == timeType {
			sv := v.(string)
			if sv == "" {
				if !optional {
					return nil, fmt.Errorf("%s may not be empty", path)
				} else {
					v = defaultValue
				}
			} else {
				_v, err := time.Parse(time.RFC3339, sv)
				if err != nil {
					_v, err = time.Parse(TDAPIDateTime, sv)
					if err != nil {
						_v, err = time.Parse(TDAPIDateTimeNumericZone, sv)
						if err != nil {
							return nil, fmt.Errorf("invalid time string %s for %s", sv, path)
						}
					}
				}
				v = _v.UTC()
			}
		} else {
			return nil, fmt.Errorf("unsupported type %s in the schema for %s", expectedType.String(), path)
		}
	case reflect.Slice:
		_path := make([]byte, len(path), len(path)+16)
		copy(_path, path)
		_path = append(_path, '[')
		h := len(_path)
		_v := v.([]interface{})
		rv := reflect.MakeSlice(expectedType, len(_v), len(_v))
		if ev.Len() == 0 {
			for i, ve := range _v {
				rv.Index(i).Set(reflect.ValueOf(ve))
			}
		} else {
			eve := ev.Index(0)
			for i, ve := range _v {
				_path = _path[0:h]
				_path = append(_path, strconv.Itoa(i)...)
				_path = append(_path, ']')
				rve, err := client.validateAndCoerceInner(string(_path), ve, eve)
				if err != nil {
					return nil, err
				}
				rv.Index(i).Set(reflect.ValueOf(rve))
			}
		}
		v = rv.Interface()
	case reflect.Map:
		_path := make([]byte, len(path), len(path)+1+16)
		copy(_path, path)
		if path != "/" {
			_path = append(_path, '/')
		}
		h := len(_path)
		_v := v.(map[string]interface{})
		for k := range _v {
			if !ev.MapIndex(reflect.ValueOf(k)).IsValid() {
				return nil, fmt.Errorf("unknown key %s under %s", k, path)
			}
		}
		rv := reflect.MakeMap(expectedType)
		for _, _k := range ev.MapKeys() {
			k := _k.String()
			eve := ev.MapIndex(_k)
			evt := eve.Type()
			if evt.Kind() == reflect.Interface {
				eve = eve.Elem()
				evt = eve.Type()
			}
			ve, ok := _v[k]
			if !ok {
				if evt == optionalType {
					ve = eve.Interface().(Optional).Default
				} else {
					return nil, fmt.Errorf("missing key %s under %s", k, path)
				}
			}
			_path = _path[0:h]
			_path = append(_path, k...)
			rve, err := client.validateAndCoerceInner(string(_path), ve, eve)
			if err != nil {
				return nil, err
			}
			rv.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(rve))
		}
		v = rv.Interface()
	}
	return v, nil
}

func (client *TDClient) validateAndCoerce(js map[string]interface{}, schema map[string]interface{}) (map[string]interface{}, error) {
	retval, err := client.validateAndCoerceInner("/", js, reflect.ValueOf(schema))
	if err == nil {
		return retval.(map[string]interface{}), err // TA: reasonably safe
	} else {
		return nil, err
	}
}

func (client *TDClient) checkedJson(resp *http.Response, schema map[string]interface{}) (map[string]interface{}, error) {
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, &APIError{
			Type:    GenericError,
			Message: "failed to read response",
			Cause:   err,
		}
	}
	js := make(map[string]interface{})
	err = json.Unmarshal(body, &js)
	if err != nil {
		return nil, &APIError{
			Type:    GenericError,
			Message: "failed to parse response: " + string(body),
			Cause:   err,
		}
	}
	js, err = client.validateAndCoerce(js, schema)
	if err != nil {
		return nil, &APIError{
			Type:    GenericError,
			Message: "failed to parse response: " + err.Error(),
			Cause:   nil,
		}
	}
	return js, nil
}

func (client *TDClient) getMessagePackDecoder(reader io.Reader) *codec.Decoder {
	return codec.NewDecoder(reader, client.mpCodec)
}

func (client *TDClient) getMessagePackEncoder(writer io.Writer) *codec.Encoder { // nolint:unused
	return codec.NewEncoder(writer, client.mpCodec)
}

func dictToValues(dict map[string]string) url.Values {
	retval := url.Values{}
	for k, v := range dict {
		retval.Set(k, v)
	}
	return retval
}

func proxyFromInterface(proxy interface{}) (retval func(*http.Request) (*url.URL, error), err error) {
	if proxy == nil {
		return nil, nil
	}
	switch proxy := proxy.(type) {
	case *url.URL:
		retval = http.ProxyURL(proxy)
	case (func(*http.Request) (*url.URL, error)):
		retval = proxy
	case string:
		var proxyUrl *url.URL
		proxyUrl, err = url.Parse(proxy)
		if err != nil {
			return
		}
		retval = http.ProxyURL(proxyUrl)
	default:
		err = errors.New("unsupported type for proxy: " + reflect.TypeOf(proxy).String())
	}
	return
}

func newDialFunc(connectionTimeout, readTimeout, sendTimeout time.Duration) func(network, address string) (net.Conn, error) {
	dialer := &net.Dialer{
		Timeout: connectionTimeout,
	}
	return func(network, address string) (net.Conn, error) {
		conn, err := dialer.Dial(network, address)
		if err != nil {
			return nil, err
		}
		return &TimeoutConn{
			Conn:         conn,
			ReadTimeout:  readTimeout,
			WriteTimeout: sendTimeout,
		}, nil
	}
}

// Creates a new TDClient instance according to the settings.
func NewTDClient(settings Settings) (*TDClient, error) {
	proxy, err := proxyFromInterface(settings.Proxy)
	if err != nil {
		return nil, err
	}
	transport := settings.Transport
	if transport == nil {
		transport = &http.Transport{
			Proxy: proxy,
			Dial: newDialFunc(
				settings.ConnectionTimeout,
				settings.ReadTimeout,
				settings.SendTimeout,
			),
			TLSClientConfig: &tls.Config{
				RootCAs: settings.RootCAs,
			},
			ResponseHeaderTimeout: settings.ReadTimeout,
			DisableCompression:    false,
		}
	}
	router := settings.Router
	if router == nil {
		router = &DefaultRouter
	}
	userAgent := "TD-Client-Go: " + CLIENT_VERSION
	if settings.UserAgent != "" {
		userAgent += "; " + settings.UserAgent
	}
	return &TDClient{
		apiKey:            settings.ApiKey,
		userAgent:         userAgent,
		router:            router,
		rootCAs:           settings.RootCAs,
		port:              settings.Port,
		connectionTimeout: settings.ConnectionTimeout,
		readTimeout:       settings.ReadTimeout,
		sendTimeout:       settings.SendTimeout,
		transport:         transport,
		headers:           settings.Headers,
		mpCodec:           &codec.MsgpackHandle{},
	}, nil
}
