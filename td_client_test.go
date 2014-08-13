package td_client

import (
	"testing"
	"time"
	"bytes"
	"io/ioutil"
	"reflect"
	"net/http"
)
var UTC, _ = time.LoadLocation("UTC")

func TestCheckSchemaSuccess(t *testing.T) {
	client := &TDClient {}
	var retval map[string]interface{}
	var err error
	retval, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": 123,
			"b": "str",
			"c": map[string]interface{} {
				"d": 1.0,
				"e": map[string]interface{} {
					"f": "2014-01-01T10:23:45+09:00",
					"g": []interface{} {
						"a",
						"b",
					},
					"h": []interface{} {
						map[string]interface{} {
							"i": "j",
						},
					},
				},
			},
		},
		map[string]interface{} {
			"a": 0,
			"b": "",
			"c": map[string]interface{} {
				"d": 0.,
				"e": map[string]interface{} {
					"f": time.Time {},
					"g": []interface{} {
						"",
					},
					"h": []map[string]string {
						map[string]string {
							"i": "",
						},
					},
				},
			},
		},
	)
	if err != nil { t.Log(err.Error()); t.FailNow() }
	if retval["a"].(int) != 123 { t.Fail() }
	if retval["b"].(string) != "str" { t.Fail() }
	if retval["c"].(map[string]interface{})["d"].(float64) != 1.0 { t.Fail() }
	if retval["c"].(map[string]interface{})["e"].(map[string]interface{})["f"].(time.Time) != time.Date(2014, 1, 1, 1, 23, 45, 0, UTC) { t.Fail() }
	if !reflect.DeepEqual(retval["c"].(map[string]interface{})["e"].(map[string]interface{})["g"].([]interface{}), []interface{} {"a", "b"}) { t.Fail() }
}

func TestCheckSchemaFail(t *testing.T) {
	client := &TDClient {}

	var err error
	_, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": 0,
			"b": "str",
		},
		map[string]interface{} {
			"a": 0,
			"b": 0,
		},
	)
	if err == nil { t.Fail() }
	_, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": 0,
			"b": "str",
		},
		map[string]interface{} {
			"a": 0.,
			"b": "",
		},
	)
	if err == nil { t.Fail() }
	_, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": 0,
			"b": map[string]interface{} {},
		},
		map[string]interface{} {
			"a": 0.,
			"b": "",
		},
	)
	if err == nil { t.Fail() }
	_, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": []interface{} { 0. },
		},
		map[string]interface{} {
			"a": "",
		},
	)
	if err == nil { t.Fail() }
	_, err = client.validateAndCoerce(
		map[string]interface{} {
			"a": []interface{} { 0., 0 },
		},
		map[string]interface{} {
			"a": []interface{} { "" },
		},
	)
	if err == nil { t.Fail() }
}

type DummyTransport struct{
	ResponseBytes []byte
}

func (t *DummyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response {
		Status: "200 OK", StatusCode: 200,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header: http.Header { "Content-Type": { "application/json" } },
		Body: ioutil.NopCloser(bytes.NewReader(t.ResponseBytes)),
		ContentLength: int64(len(t.ResponseBytes)),
		TransferEncoding: nil,
	}, nil
}

func TestServerStatus(t *testing.T) {
	client, err := NewTDClient(Settings { Transport: &DummyTransport { []byte(`{"status":"ok"}`) } })
	if err != nil { t.Log(err.Error()); t.FailNow() }
	status, err := client.ServerStatus()
	if err != nil { t.Log(err.Error()); t.FailNow() }
	if status.Status != "ok" { t.Log(err.Error()); t.Fail() }
}
