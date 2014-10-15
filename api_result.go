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
	"fmt"
	"net/url"
)

type ListResultsResultElement struct {
	Name string
	Url  string
}

type ListResultsResult []ListResultsResultElement

var listResultsSchema = map[string]interface{}{
	"results": []map[string]string{
		{
			"name": "",
			"url":  "",
		},
	},
}

func (client *TDClient) ListResults() (*ListResultsResult, error) {
	resp, err := client.get("/v3/result/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List result tables failed", nil)
	}
	js, err := client.checkedJson(resp, listResultsSchema)
	if err != nil {
		return nil, err
	}
	results := js["results"].([]map[string]string)
	retval := make(ListResultsResult, len(results))
	for i, v := range results {
		retval[i] = ListResultsResultElement{
			Name: v["name"],
			Url:  v["url"],
		}
	}
	return &retval, nil
}

func (client *TDClient) CreateResult(name, url_ string) error {
	resp, err := client.post(fmt.Sprintf("/v3/result/create/%s", url.QueryEscape(name)), url.Values{"url": {url_}})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Create result table failed", nil)
	}
	return nil
}

func (client *TDClient) DeleteResult(name string) error {
	resp, err := client.post(fmt.Sprintf("/v3/result/delete/%s", url.QueryEscape(name)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Delete result table failed", nil)
	}
	return nil
}
