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

type ServerStatusResult struct {
	Status string
}

var serverStatusSchema = map[string]interface{}{
	"status": "",
}

func (client *TDClient) ServerStatus() (*ServerStatusResult, error) {
	resp, err := client.get("/v3/system/server_status", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Server is down", nil)
	}
	js, err := client.checkedJson(resp, serverStatusSchema)
	if err != nil {
		return nil, err
	}
	return &ServerStatusResult{
		Status: js["status"].(string),
	}, nil
}
