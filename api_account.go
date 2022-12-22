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
