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
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"
	"time"
)

var UTC, _ = time.LoadLocation("UTC")

func TestCheckSchemaSuccess(t *testing.T) {
	client := &TDClient{}
	var retval map[string]interface{}
	var err error
	retval, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": 123,
			"b": "str",
			"c": map[string]interface{}{
				"d": 1.0,
				"e": map[string]interface{}{
					"f": "2014-01-01T10:23:45+09:00",
					"g": []interface{}{
						"a",
						"b",
					},
					"h": []interface{}{
						map[string]interface{}{
							"i": "j",
						},
					},
				},
			},
		},
		map[string]interface{}{
			"a": 0,
			"b": "",
			"c": map[string]interface{}{
				"d": 0.,
				"e": map[string]interface{}{
					"f": time.Time{},
					"g": []interface{}{
						"",
					},
					"h": []map[string]string{
						{
							"i": "",
						},
					},
				},
			},
		},
	)
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	if retval["a"].(int) != 123 {
		t.Fail()
	}
	if retval["b"].(string) != "str" {
		t.Fail()
	}
	if retval["c"].(map[string]interface{})["d"].(float64) != 1.0 {
		t.Fail()
	}
	if retval["c"].(map[string]interface{})["e"].(map[string]interface{})["f"].(time.Time) != time.Date(2014, 1, 1, 1, 23, 45, 0, UTC) {
		t.Fail()
	}
	if !reflect.DeepEqual(retval["c"].(map[string]interface{})["e"].(map[string]interface{})["g"].([]interface{}), []interface{}{"a", "b"}) {
		t.Fail()
	}
}

func TestCheckSchemaFail(t *testing.T) {
	client := &TDClient{}

	var err error
	_, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": 0,
			"b": "str",
		},
		map[string]interface{}{
			"a": 0,
			"b": 0,
		},
	)
	if err == nil {
		t.Fail()
	}
	_, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": 0,
			"b": "str",
		},
		map[string]interface{}{
			"a": 0.,
			"b": "",
		},
	)
	if err == nil {
		t.Fail()
	}
	_, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": 0,
			"b": map[string]interface{}{},
		},
		map[string]interface{}{
			"a": 0.,
			"b": "",
		},
	)
	if err == nil {
		t.Fail()
	}
	_, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": []interface{}{0.},
		},
		map[string]interface{}{
			"a": "",
		},
	)
	if err == nil {
		t.Fail()
	}
	_, err = client.validateAndCoerce(
		map[string]interface{}{
			"a": []interface{}{0., 0},
		},
		map[string]interface{}{
			"a": []interface{}{""},
		},
	)
	if err == nil {
		t.Fail()
	}
}

type DummyTransport struct {
	ResponseBytes []byte
}

func (t *DummyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	return &http.Response{
		Status: "200 OK", StatusCode: 200,
		Proto: "HTTP/1.1", ProtoMajor: 1, ProtoMinor: 1,
		Header:           http.Header{"Content-Type": {"application/json"}},
		Body:             ioutil.NopCloser(bytes.NewReader(t.ResponseBytes)),
		ContentLength:    int64(len(t.ResponseBytes)),
		TransferEncoding: nil,
	}, nil
}

func TestServerStatus(t *testing.T) {
	client, err := NewTDClient(Settings{Transport: &DummyTransport{[]byte(`{"status":"ok"}`)}})
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	status, err := client.ServerStatus()
	if err != nil {
		t.Log(err.Error())
		t.FailNow()
	}
	if status.Status != "ok" {
		t.Log(err.Error())
		t.Fail()
	}
}
