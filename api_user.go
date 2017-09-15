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

// ListUsersResultElement represents an item of the result of ListUsers API
type ListUsersResultElement struct {
	ID            int
	FirstName     string
	LastName      string
	Email         string
	Phone         string
	GravatarURL   string
	Administrator bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
	Name          string
	AccountOwner  bool
	Organization  string
	Roles         []string
}

// ListUsersResult is a collection of ListUsersResultElement
type ListUsersResult []ListUsersResultElement

var listUsersSchema = map[string]interface{}{
	"users": []map[string]interface{}{
		map[string]interface{}{
			"id":            0,
			"first_name":    Optional{"", ""},
			"last_name":     Optional{"", ""},
			"email":         "",
			"phone":         Optional{"", ""},
			"gravatar_url":  "",
			"administrator": false,
			"created_at":    time.Time{},
			"updated_at":    time.Time{},
			"name":          "",
			"account_owner": false,
			"organization":  Optional{"", ""},
			"roles":         []string{},
		},
	},
}

// ListAPIKeysResult represents the result of ListAPIKeys API
type ListAPIKeysResult struct {
	APIKeys []string
}

var listAPIKeysSchema = map[string]interface{}{
	"apikeys": []string{},
}

func (client *TDClient) ListUsers() (*ListUsersResult, error) {
	resp, err := client.get("/v3/user/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List users failed", nil)
	}
	js, err := client.checkedJson(resp, listUsersSchema)
	if err != nil {
		return nil, err
	}
	users := js["users"].([]map[string]interface{})
	retval := make(ListUsersResult, len(users))
	for i, v := range users {
		retval[i] = ListUsersResultElement{
			ID:            v["id"].(int),
			FirstName:     v["first_name"].(string),
			LastName:      v["last_name"].(string),
			Email:         v["email"].(string),
			Phone:         v["phone"].(string),
			GravatarURL:   v["gravatar_url"].(string),
			Administrator: v["administrator"].(bool),
			CreatedAt:     v["created_at"].(time.Time),
			UpdatedAt:     v["updated_at"].(time.Time),
			Name:          v["name"].(string),
			AccountOwner:  v["account_owner"].(bool),
			Organization:  v["organization"].(string),
			Roles:         v["roles"].([]string),
		}
	}
	return &retval, nil
}

func (client *TDClient) ListAPIKeys(email string) (*ListAPIKeysResult, error) {
	resp, err := client.get(fmt.Sprintf("/v3/user/apikey/list/%s", url.QueryEscape(email)), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List apikey failed", nil)
	}
	js, err := client.checkedJson(resp, listAPIKeysSchema)
	if err != nil {
		return nil, err
	}

	return &ListAPIKeysResult{
		APIKeys: js["apikeys"].([]string),
	}, nil
}

func (client *TDClient) UserAdd(name, org, email, password string) error {
	params := url.Values{}
	params.Set("organization", org)
	params.Set("email", email)
	params.Set("password", password)
	resp, err := client.post(fmt.Sprintf("/v3/user/add/%s", url.QueryEscape(name)), params)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "add user failed", nil)
	}
	return nil
}
