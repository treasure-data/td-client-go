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
	"strconv"
	"time"
)

type ScheduleElement struct {
	Name       string
	Cron       string
	Type       string
	Query      string
	Timezone   string
	Delay      int
	Database   string
	UserName   string
	Priority   int
	RetryLimit int
	Result     string
	NextTime   string
	CreatedAt  time.Time
}

type ListScheduleResult []ScheduleElement

var listScheduleSchema = map[string]interface{}{
	"schedules": []map[string]interface{}{
		{
			"name":        "",
			"cron":        Optional{"", "?"},
			"timezone":    "",
			"delay":       0,
			"created_at":  time.Time{},
			"type":        "",
			"query":       "",
			"database":    Optional{"", "?"},
			"user_name":   "",
			"priority":    0,
			"retry_limit": 0,
			"result":      Optional{"", "?"},
			"next_time":   Optional{"", "?"},
		},
	},
}

var scheduleResultSchema = map[string]interface{}{
	"id":          0,
	"name":        "",
	"cron":        Optional{"", "?"},
	"timezone":    "",
	"delay":       0,
	"created_at":  time.Time{},
	"type":        "",
	"query":       "",
	"database":    "",
	"user_name":   "",
	"priority":    0,
	"retry_limit": 0,
	"result":      Optional{"", "?"},
	"start":       Optional{"", "?"},
}

type ScheduleResult struct {
	ID         string
	Name       string
	Cron       string
	Type       string
	Query      string
	Timezone   string
	Delay      int
	Database   string
	UserName   string
	Priority   int
	RetryLimit int
	Result     string
	Start      string
	CreatedAt  time.Time
}

type DeleteScheduleResult struct {
	Name      string
	Cron      string
	Type      string
	Query     string
	Timezone  string
	Delay     int
	Database  string
	UserName  string
	CreatedAt time.Time
}

var deleteScheduleSchema = map[string]interface{}{
	"name":       "",
	"cron":       Optional{"", "?"},
	"timezone":   "",
	"delay":      0,
	"created_at": time.Time{},
	"type":       "",
	"query":      "",
	"database":   "",
	"user_name":  "",
}

type RunScheduleResultList []RunScheduleResult

type RunScheduleResult struct {
	ID          string
	Type        string
	ScheduledAt time.Time
}

var runScheduleResultSchema = map[string]interface{}{
	"jobs": []map[string]interface{}{
		{
			"job_id":       int64(0),
			"scheduled_at": Optional{time.Time{}, time.Time{}},
			"type":         "",
		},
	},
}

type ScheduleHistoryElement struct {
	ID               string
	Query            string
	Type             string
	URL              string
	Database         string
	Status           string
	StartAt          time.Time
	EndAt            time.Time
	ScheduledAt      time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
	UserName         string
	CPUTime          float64
	Duration         float64
	ResultSize       int
	NumRecords       int
	Result           string
	Priority         int
	RetryLimit       int
	HiveResultSchema []interface{}
	Organization     string
}

type ScheduleHistoryElementList []ScheduleHistoryElement

type ScheduleHistoryList struct {
	History ScheduleHistoryElementList
	Count   int
	From    int
	To      int
}

var scheduleHistorySchema = map[string]interface{}{
	"history": []map[string]interface{}{
		{
			"job_id":             "",
			"type":               Optional{"", "?"},
			"database":           "",
			"status":             "",
			"query":              "",
			"start_at":           Optional{time.Time{}, time.Time{}},
			"end_at":             Optional{time.Time{}, time.Time{}},
			"scheduled_at":       Optional{time.Time{}, time.Time{}},
			"created_at":         time.Time{},
			"updated_at":         time.Time{},
			"duration":           Optional{0., 0.},
			"cpu_time":           Optional{0., 0.},
			"result_size":        Optional{0, 0},
			"num_records":        Optional{0, 0},
			"user_name":          "",
			"result":             Optional{"", "?"},
			"url":                "",
			"hive_result_schema": Optional{EmbeddedJSON([]interface{}{}), nil},
			"organization":       Optional{"", "?"},
			"priority":           0,
			"retry_limit":        0,
		},
	},
	"count": Optional{0, 0},
	"to":    Optional{0, 0},
	"from":  Optional{0, 0},
}

func (client *TDClient) ListSchedules() (*ListScheduleResult, error) {
	resp, err := client.get("/v3/schedule/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List schedules failed", nil)
	}
	js, err := client.checkedJson(resp, listScheduleSchema)
	if err != nil {
		return nil, err
	}
	schedules := js["schedules"].([]map[string]interface{})
	listScheduleResult := make(ListScheduleResult, len(schedules))
	for i, v := range schedules {
		listScheduleResult[i] = ScheduleElement{
			Name:       v["name"].(string),
			Cron:       v["cron"].(string),
			Type:       v["type"].(string),
			Query:      v["query"].(string),
			Timezone:   v["timezone"].(string),
			Delay:      v["delay"].(int),
			Database:   v["database"].(string),
			UserName:   v["user_name"].(string),
			Priority:   v["priority"].(int),
			RetryLimit: v["retry_limit"].(int),
			Result:     v["result"].(string),
			NextTime:   v["next_time"].(string),
			CreatedAt:  v["created_at"].(time.Time),
		}
	}
	return &listScheduleResult, nil
}

func (client *TDClient) CreateSchedule(scheduleName string, options map[string]string) (*ScheduleResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/schedule/create/%s", url.QueryEscape(scheduleName)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for create schedule", nil)
	}
	schedule, err := client.checkedJson(resp, scheduleResultSchema)
	if err != nil {
		return nil, err
	}
	createScheduleResult := ScheduleResult{
		ID:         strconv.Itoa(schedule["id"].(int)),
		Name:       schedule["name"].(string),
		Cron:       schedule["cron"].(string),
		Type:       schedule["type"].(string),
		Query:      schedule["query"].(string),
		Timezone:   schedule["timezone"].(string),
		Delay:      schedule["delay"].(int),
		Database:   schedule["database"].(string),
		UserName:   schedule["user_name"].(string),
		Priority:   schedule["priority"].(int),
		RetryLimit: schedule["retry_limit"].(int),
		Result:     schedule["result"].(string),
		Start:      schedule["start"].(string),
		CreatedAt:  schedule["created_at"].(time.Time),
	}
	return &createScheduleResult, nil
}

func (client *TDClient) DeleteSchedule(scheduleName string) (*DeleteScheduleResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/schedule/delete/%s", url.QueryEscape(scheduleName)), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for delete schedule", nil)
	}
	schedule, err := client.checkedJson(resp, deleteScheduleSchema)
	if err != nil {
		return nil, err
	}
	deleteScheduleResult := DeleteScheduleResult{
		Name:      schedule["name"].(string),
		Cron:      schedule["cron"].(string),
		Type:      schedule["type"].(string),
		Query:     schedule["query"].(string),
		Timezone:  schedule["timezone"].(string),
		Delay:     schedule["delay"].(int),
		Database:  schedule["database"].(string),
		UserName:  schedule["user_name"].(string),
		CreatedAt: schedule["created_at"].(time.Time),
	}
	return &deleteScheduleResult, nil
}

func (client *TDClient) UpdateSchedule(scheduleName string, options map[string]string) (*ScheduleResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/schedule/update/%s", url.QueryEscape(scheduleName)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for update schedule", nil)
	}
	schedule, err := client.checkedJson(resp, scheduleResultSchema)
	if err != nil {
		return nil, err
	}
	updateScheduleResult := ScheduleResult{
		ID:         strconv.Itoa(schedule["id"].(int)),
		Name:       schedule["name"].(string),
		Cron:       schedule["cron"].(string),
		Type:       schedule["type"].(string),
		Query:      schedule["query"].(string),
		Timezone:   schedule["timezone"].(string),
		Delay:      schedule["delay"].(int),
		Database:   schedule["database"].(string),
		UserName:   schedule["user_name"].(string),
		Priority:   schedule["priority"].(int),
		RetryLimit: schedule["retry_limit"].(int),
		Result:     schedule["result"].(string),
		Start:      schedule["start"].(string),
		CreatedAt:  schedule["created_at"].(time.Time),
	}
	return &updateScheduleResult, nil
}

func (client *TDClient) RunSchedule(scheduleName string, runTime string, options map[string]string) (*RunScheduleResultList, error) {
	resp, err := client.post(fmt.Sprintf("/v3/schedule/run/%s/%s", url.QueryEscape(scheduleName), url.QueryEscape(runTime)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for run schedule", nil)
	}
	js, err := client.checkedJson(resp, runScheduleResultSchema)
	if err != nil {
		return nil, err
	}
	jobs := js["jobs"].([]map[string]interface{})
	runResultList := make(RunScheduleResultList, len(jobs))
	for i, v := range jobs {
		runResultList[i] = RunScheduleResult{
			ID:          strconv.FormatInt(v["job_id"].(int64), 10),
			Type:        v["type"].(string),
			ScheduledAt: v["scheduled_at"].(time.Time),
		}
	}

	return &runResultList, nil
}

func (client *TDClient) ScheduleHistory(scheduleName string, options map[string]string) (*ScheduleHistoryList, error) {
	resp, err := client.get(fmt.Sprintf("/v3/schedule/history/%s", scheduleName), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for get schedule history", nil)
	}
	js, err := client.checkedJson(resp, scheduleHistorySchema)
	if err != nil {
		return nil, err
	}
	scheduleHistoryList := ScheduleHistoryList{}
	history := js["history"].([]map[string]interface{})
	scheduleHistory := make(ScheduleHistoryElementList, len(history))

	for i, v := range history {
		hiveResultSchema, _ := v["hive_result_schema"].([]interface{})
		scheduleHistory[i] = ScheduleHistoryElement{
			ID:               v["job_id"].(string),
			Query:            v["query"].(string),
			Type:             v["type"].(string),
			Status:           v["status"].(string),
			ScheduledAt:      v["scheduled_at"].(time.Time),
			CreatedAt:        v["created_at"].(time.Time),
			UpdatedAt:        v["updated_at"].(time.Time),
			StartAt:          v["start_at"].(time.Time),
			EndAt:            v["end_at"].(time.Time),
			Duration:         v["duration"].(float64),
			CPUTime:          v["cpu_time"].(float64),
			ResultSize:       v["result_size"].(int),
			NumRecords:       v["num_records"].(int),
			Result:           v["result"].(string),
			Priority:         v["priority"].(int),
			RetryLimit:       v["retry_limit"].(int),
			HiveResultSchema: hiveResultSchema,
			UserName:         v["user_name"].(string),
			URL:              v["url"].(string),
			Database:         v["database"].(string),
			Organization:     v["organization"].(string),
		}
	}
	scheduleHistoryList.History = scheduleHistory
	scheduleHistoryList.From = js["from"].(int)
	scheduleHistoryList.To = js["to"].(int)
	scheduleHistoryList.Count = js["count"].(int)
	return &scheduleHistoryList, nil
}
