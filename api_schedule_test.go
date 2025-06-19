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
	"testing"
	"time"
)

const TestDatabaseName = "test_databse"
const TestScheduleName = "test_schedule"
const TestResultTableName = "test_result_table"
const TestTableName = "test_table"

func TestListSchedules(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"schedules":[{"id": 999999999, "executing_user_id": 1, "description": "blah", "name":"test_query","cron":null,"timezone":"UTC","delay":0,"created_at":"2017-03-27T09:39:42Z","type":"presto","query":"SELECT * FROM test_table","database":"test","user_name":"Test User","priority":0,"retry_limit":0,"result":"","next_time":null}]}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	scheduleList, err := client.ListSchedules()
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if scheduleList == nil {
		t.Fatal("schedule list is undefined")
	}
	t.Logf("TestListSchedules: %+v", scheduleList)
}

func TestCreateSchedule(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_sdk_query","cron":null,"timezone":"UTC","delay":0,"created_at":"2017-04-26T09:54:20Z","type":"presto","query":"select * from test","database":"test_db","user_name":"Test User","priority":0,"retry_limit":0,"result":"","id":234451,"start":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	scheduleParam := map[string]string{
		"timezone": "UTC",
		"result":   fmt.Sprintf("td://@/%s/%s?mode=replace", TestDatabaseName, TestResultTableName),
		"cron":     "@monthly",
		"type":     "presto",
		"database": TestDatabaseName,
		"query":    fmt.Sprintf("SELECT * FROM %s", TestTableName),
	}
	createResult, err := client.CreateSchedule(TestScheduleName, scheduleParam)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if createResult == nil {
		t.Fatal("schedule create result is undefined")
	}
	t.Logf("TestCreateSchedule: %+v", createResult)
}

func TestUpdateSchedule(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_sdk_query","cron":null,"timezone":"UTC","delay":0,"created_at":"2017-04-26T09:54:20Z","type":"presto","query":"select * from test","database":"test_db","user_name":"Test User","priority":2,"retry_limit":0,"result":"","id":234451,"start":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	scheduleParam := map[string]string{
		"timezone": "Asia/Tokyo",
		"type":     "hive",
	}
	updateResult, err := client.UpdateSchedule(TestScheduleName, scheduleParam)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("TestUpdateSchedule: %+v", updateResult)
}

func TestRunSchedule(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"jobs":[{"job_id":11111111111,"type":"presto","scheduled_at":"2017-04-26 11:57:00 UTC"}]}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	var runTime string = time.Now().String()
	runResultList, err := client.RunSchedule(TestScheduleName, runTime, nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(*runResultList) != 1 {
		t.Fatalf("want 1 job, got %d", len(*runResultList))
	}
	runResult := (*runResultList)[0]
	if runResult.ID != "11111111111" {
		t.Fatalf("want job ID 11111111111, got %q", runResult.ID)
	}
	t.Logf("TestRunSchedule: %+v", runResultList)
}

func TestScheduleHistory(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{
			"history": [
				{
					"query": "SELECT * FROM test_table2;",
					"type": "presto",
					"priority": -2,
					"retry_limit": 0,
					"duration": 10,
					"status": "success",
					"cpu_time": null,
					"result_size": 20,
					"job_id": "1111111",
					"created_at": "2017-04-26 08:39:43 UTC",
					"updated_at": "2017-04-26 08:39:53 UTC",
					"start_at": "2017-04-26 08:39:43 UTC",
					"end_at": "2017-04-26 08:39:53 UTC",
					"num_records": 0,
					"database": "test_db",
					"user_name": "Test User",
					"result": "td://@/test_db/test_table",
					"url": "https://console.treasuredata.com/jobs/1111111",
					"hive_result_schema": "[[\"aaaaaaaa\", \"bbbbbb\"]]",
					"organization": null,
					"scheduled_at": "2017-04-26 08:39:00 UTC"
				},
				{
					"query": "SELECT * FROM test_table;",
					"type": "presto",
					"priority": -2,
					"retry_limit": 0,
					"duration": 10,
					"status": "success",
					"cpu_time": null,
					"result_size": 20,
					"job_id": "99999999",
					"created_at": "2017-04-26 07:58:27 UTC",
					"updated_at": "2017-04-26 07:58:38 UTC",
					"start_at": "2017-04-26 07:58:28 UTC",
					"end_at": "2017-04-26 07:58:38 UTC",
					"num_records": 0,
					"database": "test_db",
					"user_name": "Test User",
					"result": "td://@/test_db/test_table",
					"url": "https://console.treasuredata.com/jobs/99999999",
					"hive_result_schema": "[[\"ccccc\", \"ddddddd\"]]",
					"organization": null,
					"scheduled_at": "2017-04-26 07:58:00 UTC"
				}
			],
			"count": 2,
			"from": 0,
			"to": 20
		}`)}})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	historyResultList, err := client.ScheduleHistory(TestScheduleName, nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(historyResultList.History) < 1 {
		t.Fatalf("failed get history %d", len(historyResultList.History))
	}
	t.Logf("TestScheduleHistory: %+v", historyResultList)
}

func TestDeleteSchedule(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{
			"name": "test_schedule",
			"cron": null,
			"timezone": "UTC",
			"delay": 0,
			"created_at": "2016-11-25T11:59:19Z",
			"type": "presto",
			"query": "SELECT * FROM test_table",
			"database": "test_db",
			"user_name": "Test User"
		}`)}})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	deleteResult, err := client.DeleteSchedule(TestScheduleName)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if deleteResult == nil {
		t.Fatal("schedule delete result is undefined")
	}
	t.Logf("TestDeleteSchedule: %+v", deleteResult)
}
