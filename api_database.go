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
	"time"
)

// ListDataBasesResultElement represents an item of the result of
// ListDatabases API call
type ListDataBasesResultElement struct {
	Id              string
	UserId          int
	Description     string
	Name            string
	Organization    string
	Count           int
	CreatedAt       time.Time
	UpdatedAt       time.Time
	Permission      string
	DeleteProtected bool
}

// ListDataBasesResult is a collection of ListDataBasesResultElement
type ListDataBasesResult []ListDataBasesResultElement

var listDatabasesSchema = map[string]interface{}{
	"databases": []map[string]interface{}{
		{
			"id":               "",
			"user_id":          0,
			"description":      Optional{"", nil},
			"name":             "",
			"organization":     Optional{"", ""},
			"count":            0,
			"created_at":       time.Time{},
			"updated_at":       time.Time{},
			"permission":       "",
			"delete_protected": false,
		},
	},
}

func (client *TDClient) ShowDatabase(dbname string) (*ListDataBasesResultElement, error) {
	result, err := client.ListDatabases()
	if err != nil {
		return nil, err
	}

	for _, db := range *result {
		if db.Name == dbname {
			return &db, nil
		}
	}

	return nil, fmt.Errorf("Database '%s' does not exist", dbname)
}

func (client *TDClient) ListDatabases() (*ListDataBasesResult, error) {
	resp, err := client.get("/v3/database/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List databases failed", nil)
	}
	js, err := client.checkedJson(resp, listDatabasesSchema)
	if err != nil {
		return nil, err
	}
	databases := js["databases"].([]map[string]interface{})
	retval := make(ListDataBasesResult, len(databases))
	for i, v := range databases {
		retval[i] = ListDataBasesResultElement{
			Id:              v["id"].(string),
			UserId:          v["user_id"].(int),
			Description:     v["description"].(string),
			Name:            v["name"].(string),
			Organization:    v["organization"].(string),
			Count:           v["count"].(int),
			CreatedAt:       v["created_at"].(time.Time),
			UpdatedAt:       v["updated_at"].(time.Time),
			Permission:      v["permission"].(string),
			DeleteProtected: v["delete_protected"].(bool),
		}
	}
	return &retval, nil
}

func (client *TDClient) DeleteDatabase(db string) error {
	resp, err := client.post(fmt.Sprintf("/v3/database/delete/%s", url.QueryEscape(db)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Delete database failed", nil)
	}
	return nil
}

func (client *TDClient) CreateDatabase(db string, options map[string]string) error {
	resp, err := client.post(fmt.Sprintf("/v3/database/create/%s", url.QueryEscape(db)), dictToValues(options))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Create database failed", nil)
	}
	return nil
}
