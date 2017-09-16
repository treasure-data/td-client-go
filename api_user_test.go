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
	"testing"
)

func TestAuthenticate(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(authenticateResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	authenticate, err := client.Authenticate("hogefuga@github.com", "123456789")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Log(authenticate)
}

func TestListUsers(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(listUsersResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	userList, err := client.ListUsers()
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(*userList) != 3 {
		t.Fatal("not expected user count")
	}
	t.Log(userList)
}

func TestListAPIKeys(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(listAPIKeysResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	apiKeys, err := client.ListAPIKeys("hogefuga@github.com")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(apiKeys.APIKeys) != 2 {
		t.Fatal("not expected apikeys count")
	}
	t.Log(apiKeys)
}

func TestAddUser(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(addUserResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.AddUser("Test User", "td", "hogefuga@github.com", "123456789")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestRemoveUser(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(removeUserResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.RemoveUser("hogefuga@github.com")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

const authenticateResponse = `
{
    "name": "Test User",
    "apikey": "0000/hbfr73pix9abuciofzg8gig55cndl8clpwaz2akb"
}
`

const listUsersResponse = `
{
    "users": [
        {
            "id": 1,
            "first_name": "HOGE",
            "last_name": "FUGA",
            "email": "hogefuga@github.com",
            "phone": "00-00-0000-0000",
            "gravatar_url": "https://secure.gravatar.com/avatar/1c56a29a2f0c3114822fa47c4308410f?size=80",
            "administrator": true,
            "created_at": "2016-05-17T09:26:38Z",
            "updated_at": "2017-06-07T22:09:34Z",
            "name": "HOGE FUGA",
            "account_owner": true,
            "organization": null,
            "roles": []
        },
        {
            "id": 2,
            "first_name": "FIZZ",
            "last_name": "BUZZ",
            "email": "fizzbuzz@github.com",
            "phone": null,
            "gravatar_url": "https://secure.gravatar.com/avatar/11b59b7feed4ee2995328a666ae2eb3f?size=80",
            "administrator": false,
            "created_at": "2016-05-30T09:20:18Z",
            "updated_at": "2017-09-06T02:50:23Z",
            "name": "FIZZ BUZZ",
            "account_owner": false,
            "organization": null,
            "roles": []
        },
        {
            "id": 3,
            "first_name": null,
            "last_name": null,
            "email": "foovar@github.com",
            "phone": null,
            "gravatar_url": "https://secure.gravatar.com/avatar/304d83e6eb7fca0b44e3a3a4edb1e063?size=80",
            "administrator": true,
            "created_at": "2016-05-30T09:20:35Z",
            "updated_at": "2016-07-13T09:42:32Z",
            "name": "foovar@github.com",
            "account_owner": false,
            "organization": null,
            "roles": []
        }
    ]
}
`

const listAPIKeysResponse = `
{
    "apikeys": [
        "0000/hogehogehogehogehogehogehogehogehogehoge",
        "0000/fugafugafugafugafugafugafugafugafugafuga"
    ]
}
`
const addUserResponse = `
{
    "name":"Test User"
}
`
const removeUserResponse = `
{
    "user":"hogefuga@github.com"
}
`
