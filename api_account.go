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
	"net/url"
	"time"
)

// ShowAccountResult stores the result of `ShowAccountResult` API call
type ShowAccountResult struct {
	Id              int
	Plan            int
	StorageSize     int
	GuaranteedCores int
	MaximumCores    int
	CreatedAt       time.Time
}

// AccountCoreUtilizationResult stores the result of `AccountCoreUtiizationResult` API call
type AccountCoreUtilizationResult struct {
	From     time.Time
	To       time.Time
	Interval int
	History  []interface{}
}

var showAccountSchema = map[string]interface{}{
	"account": map[string]interface{}{
		"id":               0,
		"plan":             0,
		"storage_size":     0,
		"guaranteed_cores": 0,
		"maximum_cores":    0,
		"created_at":       time.Time{},
		"presto_plan":      0.,
	},
}

var accountCoreUtilizationSchema = map[string]interface{}{
	"from": map[string]interface{}{
		"from":     time.Time{},
		"to":       time.Time{},
		"interval": 0,
		"history":  []interface{}{},
	},
}

// ShowAccount returns the information about the current account
func (client *TDClient) ShowAccount() (*ShowAccountResult, error) {
	resp, err := client.get("/v3/account/show", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Show account failed", nil)
	}
	js, err := client.checkedJson(resp, showAccountSchema)
	if err != nil {
		return nil, err
	}
	a := js["account"].(map[string]interface{}) // TA: reasonably safe
	return &ShowAccountResult{
		Id:              a["id"].(int),
		Plan:            a["plan"].(int),
		StorageSize:     a["storage_size"].(int),
		GuaranteedCores: a["guaranteed_cores"].(int),
		MaximumCores:    a["maximum_cores"].(int),
		CreatedAt:       a["created_at"].(time.Time),
	}, nil
}

// AccountCoreUtilization returns the utilization statistics of the current
// account
func (client *TDClient) AccountCoreUtilization(from time.Time, to time.Time) (*AccountCoreUtilizationResult, error) {
	params := url.Values{}
	if !from.IsZero() {
		params.Set("from", from.UTC().Format(TDAPIDateTime))
	}
	if !to.IsZero() {
		params.Set("to", to.UTC().Format(TDAPIDateTime))
	}
	resp, err := client.get("/v3/account/core_utilization", params)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Core utilization failed", nil)
	}
	js, err := client.checkedJson(resp, accountCoreUtilizationSchema)
	if err != nil {
		return nil, err
	}
	return &AccountCoreUtilizationResult{
		From:     js["from"].(time.Time),
		To:       js["to"].(time.Time),
		Interval: js["interval"].(int),
		History:  js["history"].([]interface{}),
	}, nil
}
