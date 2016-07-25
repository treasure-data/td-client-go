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
	"os"
	"testing"

	td_client "github.com/treasure-data/td-client-go"
)

func TestServerStatus(t *testing.T) {
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	serverStatus, err := client.ServerStatus()
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if serverStatus.Status != "ok" {
		t.Fatalf("server status is : %s", serverStatus.Status)
	} else {
		t.Logf("server status: %s", serverStatus.Status)
	}
}
