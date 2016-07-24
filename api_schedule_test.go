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
	"os"
	"testing"
	"time"

	"github.com/treasure-data/td-client-go"
)

const TestDatabaseName = "test_databse"
const TestScheduleName = "test_schedule"
const TestResultTableName = "test_result_table"
const TestTableName = "test_table"

func TestMain(m *testing.M) {
	apiKey := os.Getenv("TD_CLIENT_API_KEY")

	client, _ := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
	})
	client.CreateDatabase(TestDatabaseName, nil)
	client.CreateLogTable(TestDatabaseName, TestTableName)
	code := m.Run()
	client.DeleteDatabase(TestDatabaseName)
	os.Exit(code)
}

func TestListSchedules(t *testing.T) {
	apiKey := os.Getenv("TD_CLIENT_API_KEY")

	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
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
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
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
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
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
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	var runTime string
	runTime = time.Now().String()
	runResultList, err := client.RunSchedule(TestScheduleName, runTime, nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	for _, runResult := range *runResultList {
		for {
			jobStatus, _ := client.JobStatus(runResult.ID)
			if jobStatus == "success" || jobStatus != "killed" {
				break
			} else if jobStatus != "error" {
				t.Fatalf("failed run status %s", jobStatus)
			}
			time.Sleep(10000)
		}
	}
	t.Logf("TestRunSchedule: %+v", runResultList)
}

func TestScheduleHistory(t *testing.T) {
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
	})
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
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := td_client.NewTDClient(td_client.Settings{
		ApiKey: apiKey,
	})
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
