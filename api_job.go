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
	"io"
	"net/url"
	"strconv"
	"time"
)

type ListJobsResultElement struct {
	Id         string
	Type       string
	Database   string
	Status     string
	Query      string
	Duration   int
	CreatedAt  time.Time
	UpdatedAt  time.Time
	StartAt    time.Time
	EndAt      time.Time
	CpuTime    float64
	ResultSize int
	NumRecords int
	ResultUrl  string
	Priority   int
	RetryLimit int
}

type ListJobsResultElements []ListJobsResultElement

type ListJobsResult struct {
	ListJobsResultElements ListJobsResultElements
	Count                  int
	From                   string
	To                     string
}

var listJobsSchema = map[string]interface{}{
	"jobs": []map[string]interface{}{
		{
			"job_id":             "",
			"type":               Optional{"", "?"},
			"database":           "",
			"status":             "",
			"query":              "",
			"start_at":           time.Time{},
			"end_at":             Optional{time.Time{}, time.Time{}},
			"created_at":         time.Time{},
			"updated_at":         time.Time{},
			"duration":           Optional{0., 0.},
			"cpu_time":           Optional{0., 0.},
			"result_size":        Optional{0, 0},
			"num_records":        Optional{0, 0},
			"user_name":          "",
			"result":             "",
			"url":                "",
			"hive_result_schema": Optional{"", "?"},
			"organization":       Optional{"", "?"},
			"priority":           0,
			"retry_limit":        0,
		},
	},
	"count": Optional{0, 0},
	"to":    Optional{"", "?"},
	"from":  Optional{"", "?"},
}

var jobStatusSchema = map[string]interface{}{
	"status":      "",
	"job_id":      "",
	"start_at":    Optional{time.Time{}, time.Time{}},
	"created_at":  time.Time{},
	"updated_at":  time.Time{},
	"end_at":      Optional{time.Time{}, time.Time{}},
	"duration":    Optional{0., 0.},
	"cpu_time":    Optional{0., 0.},
	"result_size": Optional{0, 0},
	"num_records": Optional{0, 0},
}

type ShowJobResultDebugElement struct {
	CmdOut string
	StdErr string
}

// ShowJobResult stores the result of `ShowJobResult` API call.
type ShowJobResult struct {
	Id               string
	Type             string
	Database         string
	UserName         string
	Status           string
	Query            string
	Debug            ShowJobResultDebugElement
	Url              string
	Duration         int
	CreatedAt        time.Time
	UpdatedAt        time.Time
	StartAt          time.Time
	EndAt            time.Time
	CpuTime          float64
	ResultSize       int
	NumRecords       int
	ResultUrl        string
	Priority         int
	RetryLimit       int
	HiveResultSchema []interface{}
}

var showJobSchema = map[string]interface{}{
	"job_id":       "",
	"type":         Optional{"", "?"},
	"organization": Optional{"", ""},
	"user_name":    "",
	"database":     "",
	"status":       "",
	"query":        "",
	"debug": map[string]interface{}{
		"cmdout": Optional{"", ""},
		"stderr": Optional{"", ""},
	},
	"url":                "",
	"duration":           Optional{0, 0},
	"created_at":         time.Time{},
	"updated_at":         time.Time{},
	"start_at":           Optional{time.Time{}, time.Time{}},
	"end_at":             Optional{time.Time{}, time.Time{}},
	"cpu_time":           Optional{0., 0.},
	"result_size":        Optional{0, 0},
	"num_records":        Optional{0, 0},
	"result":             "",
	"priority":           0,
	"retry_limit":        0,
	"hive_result_schema": Optional{EmbeddedJSON([]interface{}{}), nil},
}

type Query struct {
	Type       string
	Query      string
	ResultUrl  string
	Priority   int
	RetryLimit int
}

var submitJobSchema = map[string]interface{}{
	"job":      "",
	"job_id":   "",
	"database": "",
}

func (client *TDClient) ListJobs() (*ListJobsResult, error) {
	resp, err := client.get("/v3/job/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List jobs failed", nil)
	}
	js, err := client.checkedJson(resp, listJobsSchema)
	if err != nil {
		return nil, err
	}
	jobs := js["jobs"].([]map[string]interface{})
	listJobsResult := ListJobsResult{}
	retval := make(ListJobsResultElements, len(jobs))
	for i, v := range jobs {
		retval[i] = ListJobsResultElement{
			Id:         v["job_id"].(string),
			Type:       v["type"].(string),
			Database:   v["database"].(string),
			Status:     v["status"].(string),
			Query:      v["query"].(string),
			StartAt:    v["start_at"].(time.Time),
			EndAt:      v["end_at"].(time.Time),
			CpuTime:    v["cpu_time"].(float64),
			ResultSize: v["result_size"].(int),
			NumRecords: v["num_records"].(int),
			ResultUrl:  v["result"].(string),
			Priority:   v["priority"].(int),
			RetryLimit: v["retry_limit"].(int),
		}
	}
	listJobsResult.ListJobsResultElements = retval
	listJobsResult.Count = js["count"].(int)
	listJobsResult.From = js["from"].(string)
	listJobsResult.To = js["to"].(string)
	return &listJobsResult, nil
}

func (client *TDClient) ShowJob(jobId string) (*ShowJobResult, error) {
	resp, err := client.get(fmt.Sprintf("/v3/job/show/%s", url.QueryEscape(jobId)), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Show job failed", nil)
	}
	js, err := client.checkedJson(resp, showJobSchema)
	if err != nil {
		return nil, err
	}
	typeStr := js["type"].(string)
	hiveResultSchema, _ := js["hive_result_schema"].([]interface{})
	return &ShowJobResult{
		Id:       js["job_id"].(string),
		Type:     typeStr,
		Database: js["database"].(string),
		UserName: js["user_name"].(string),
		Status:   js["status"].(string),
		Query:    js["query"].(string),
		Debug: ShowJobResultDebugElement{
			CmdOut: js["debug"].(map[string]interface{})["cmdout"].(string),
			StdErr: js["debug"].(map[string]interface{})["stderr"].(string),
		},
		Url:              js["url"].(string),
		CreatedAt:        js["created_at"].(time.Time),
		UpdatedAt:        js["updated_at"].(time.Time),
		StartAt:          js["start_at"].(time.Time),
		EndAt:            js["end_at"].(time.Time),
		CpuTime:          js["cpu_time"].(float64),
		ResultSize:       js["result_size"].(int),
		NumRecords:       js["num_records"].(int),
		ResultUrl:        js["result"].(string),
		Priority:         js["priority"].(int),
		RetryLimit:       js["retry_limit"].(int),
		HiveResultSchema: hiveResultSchema,
	}, nil
}

func (client *TDClient) JobStatus(jobId string) (string, error) {
	resp, err := client.get(fmt.Sprintf("/v3/job/status/%s", url.QueryEscape(jobId)), nil)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", client.buildError(resp, -1, "Get job status failed", nil)
	}
	js, err := client.checkedJson(resp, jobStatusSchema)
	if err != nil {
		return "", err
	}
	return js["status"].(string), nil
}

func (client *TDClient) JobResult(jobId string, format string, reader func(io.Reader) error) error {
	resp, err := client.get(fmt.Sprintf("/v3/job/result/%s", url.QueryEscape(jobId)), url.Values{"format": {format}})
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Get job result failed", nil)
	}
	return reader(resp.Body)
}

func (client *TDClient) JobResultEach(jobId string, reader func(interface{}) error) error {
	return client.JobResult(jobId, "msgpack", func(r io.Reader) error {
		dec := client.getMessagePackDecoder(r)
		for {
			v := (interface{})(nil)
			err := dec.Decode(&v)
			if err != nil {
				if err == io.EOF {
					break
				}
				return &APIError{
					Type:    GenericError,
					Message: "Invalid MessagePack stream",
					Cause:   err,
				}
			}
			err = reader(v)
			if err != nil {
				return &APIError{
					Type:    GenericError,
					Message: "Reader returned error status",
					Cause:   err,
				}
			}
		}
		return nil
	})
}

func (client *TDClient) KillJob(jobId string) error {
	resp, err := client.post(fmt.Sprintf("/v3/job/kill/%s", url.QueryEscape(jobId)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Kill job failed", nil)
	}
	return nil
}

func (client *TDClient) SubmitQuery(db string, q Query) (string, error) {
	params := url.Values{}
	params.Set("query", q.Query)
	if q.ResultUrl != "" {
		params.Set("result", q.ResultUrl)
	}
	if q.Priority >= 0 {
		params.Set("priority", strconv.Itoa(q.Priority))
	}
	if q.RetryLimit >= 0 {
		params.Set("retry_limit", strconv.Itoa(q.RetryLimit))
	}
	resp, err := client.post(fmt.Sprintf("/v3/job/issue/%s/%s", url.QueryEscape(q.Type), url.QueryEscape(db)), params)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", client.buildError(resp, -1, "Query failed", nil)
	}
	js, err := client.checkedJson(resp, submitJobSchema)
	if err != nil {
		return "", err
	}
	return js["job_id"].(string), nil
}

func (client *TDClient) SubmitExportJob(db string, table string, storageType string, options map[string]string) (string, error) {
	params := dictToValues(options)
	params.Set("storage_type", storageType)
	resp, err := client.post(fmt.Sprintf("/v3/export/run/%s/%s", url.QueryEscape(db), url.QueryEscape(table)), params)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", client.buildError(resp, -1, "Export failed", nil)
	}
	js, err := client.checkedJson(resp, submitJobSchema)
	if err != nil {
		return "", err
	}
	return js["job_id"].(string), nil
}

func (client *TDClient) SubmitPartialDeleteJob(db string, table string, to time.Time, from time.Time, options map[string]string) (string, error) {
	params := dictToValues(options)
	if !to.IsZero() {
		params.Set("to", to.UTC().Format(TDAPIDateTime))
	}
	if !from.IsZero() {
		params.Set("from", from.UTC().Format(TDAPIDateTime))
	}
	resp, err := client.post(fmt.Sprintf("/v3/table/partialdelete/%s/%s", url.QueryEscape(db), url.QueryEscape(table)), params)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", client.buildError(resp, -1, "Partial delete failed", nil)
	}
	js, err := client.checkedJson(resp, submitJobSchema)
	if err != nil {
		return "", err
	}
	return js["job_id"].(string), nil
}
