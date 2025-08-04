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

func TestListJobs(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"jobs":[{"query":"SELECT COUNT (*) FROM www_access","type":"hive","priority":0,"retry_limit":0,"duration":2,"status":"killed","cpu_time":null,"result_size":null,"job_id":"9999991","created_at":"2016-07-26 08:14:46 UTC","updated_at":"2016-07-26 08:14:48 UTC","start_at":"2016-07-26 08:14:46 UTC","end_at":"2016-07-26 08:14:48 UTC","num_records":null,"database":"sample_datasets","user_name":"hogehoge@hoge.co.jp","result":"","url":"https://console.treasuredata.com/jobs/9999991","hive_result_schema":null,"organization":null},{"query":"SELECT COUNT (*) FROM www_access","type":"hive","priority":0,"retry_limit":0,"duration":2,"status":"killed","cpu_time":null,"result_size":null,"job_id":"9999992","created_at":"2016-07-26 08:14:46 UTC","updated_at":"2016-07-26 08:14:48 UTC","start_at":"2016-07-26 08:14:46 UTC","end_at":"2016-07-26 08:14:48 UTC","num_records":null,"database":"sample_datasets","user_name":"hogehoge@hoge.co.jp","result":"","url":"https://console.treasuredata.com/jobs/9999992","hive_result_schema":null,"organization":null},{"query":"SELECT COUNT (*) FROM www_access","type":"presto","priority":0,"retry_limit":0,"duration":0,"status":"success","cpu_time":null,"result_size":24,"job_id":"9999993","created_at":"2016-07-26 10:25:35 UTC","updated_at":"2016-07-26 10:25:35 UTC","start_at":"2016-07-26 10:25:35 UTC","end_at":"2016-07-26 10:25:35 UTC","num_records":1,"database":"sample_datasets","user_name":"hogehoge@hoge.co.jp","result":"","url":"https://console.treasuredata.com/jobs/9999993","hive_result_schema":"[[\"_col0\", \"bigint\"]]","organization":null},{"query":"SELECT COUNT (*) FROM www_access","type":"presto","priority":0,"retry_limit":0,"duration":1,"status":"success","cpu_time":null,"result_size":24,"job_id":"9999994","created_at":"2016-07-26 10:25:16 UTC","updated_at":"2016-07-26 10:25:17 UTC","start_at":"2016-07-26 10:25:16 UTC","end_at":"2016-07-26 10:25:17 UTC","num_records":1,"database":"sample_datasets","user_name":"hogehoge@hoge.co.jp","result":"","url":"https://console.treasuredata.com/jobs/9999994","hive_result_schema":"[[\"_col0\", \"bigint\"]]","organization":null}],"count":4,"from":null,"to":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobList, err := client.ListJobs()
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(jobList.ListJobsResultElements) < 1 {
		t.Fatal("job list is undefined")
	}
	if len(jobList.ListJobsResultElements) != jobList.Count {
		t.Fatal("job list has diff with count")
	}
}

func TestShowJob(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"query":"SELECT COUNT (*) FROM www_access","type":"presto","priority":0,"retry_limit":0,"duration":1,"status":"success","cpu_time":null,"result_size":24,"job_id":"9999999","created_at":"2016-07-26 08:29:33 UTC","updated_at":"2016-07-26 08:29:34 UTC","start_at":"2016-07-26 08:29:33 UTC","end_at":"2016-07-26 08:29:34 UTC","num_records":1,"database":"sample_datasets","user_name":"hogehoge@hoge.co.jp","result":"","url":"https://console.treasuredata.com/jobs/9999999","hive_result_schema":"[[\"_col0\", \"bigint\"]]","organization":null,"debug":{"cmdout":"started at 2016-07-26T08:29:33Z\nexecuting query: SELECT COUNT (*) FROM www_access\n**\n** WARNING: time index filtering is not set on \n** This query could be very slow as a result.\n** Please see https://docs.treasuredata.com/articles/presto-performance-tuning#leveraging-time-based-partitioning\n**\nQuery plan:\n- Stage-0\n    OutputPartitioning: \n    DistributedExecution: \n    -> Output[6]\n        Columns: _col0 = count:bigint\n        -> FinalAggregate[11]\n            Aggregations: count:bigint = \"count\"(\"count_3\")\n            -> RemoteSource[10]\n                Sources: Stage-1\n- Stage-1\n    OutputPartitioning: \n    DistributedExecution: \n    -> PartialAggregate[9]\n        Aggregations: count_3:bigint = \"count\"(*)\n        -> TableScan[0]\n            Table: \n            Columns: \nStarted fetching results.\n1 rows.\n2016-07-26 08:29:34 -- memory:0B, peak memory:48B, queued time:872.47ms\n20160726_082933_44944_fff3t                    944.04ms  rows  bytes bytes/sec done   total             \n[0] output <- aggregation <- [1]               FINISHED     0     0B      0B/s    0 /     1             \n [1] aggregation <- sample_datasets.www_access FINISHED 5,000     0B      0B/s    6 /     6 [*] FullScan\nfinished at 2016-07-26T08:29:34Z\n","stderr":null}}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobDesc, err := client.ShowJob("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if jobDesc.Id != "9999999" {
		t.Fatal("job id has diff with request job id")
	}
}

func TestJobStatusError(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"status":"error","cpu_time":null,"result_size":0,"duration":0,"job_id":"9999999","created_at":"2016-07-20 06:53:42 UTC","updated_at":"2016-07-20 06:53:43 UTC","start_at":"2016-07-20 06:53:43 UTC","end_at":"2016-07-20 06:53:43 UTC","num_records":10}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobStatus, err := client.JobStatus("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Job Status is %s", jobStatus)
	if jobStatus != "error" {
		t.Fatalf("Unexpected job status: %s", jobStatus)
	}
}

func TestJobStatusKilled(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"status":"killed","cpu_time":null,"result_size":null,"duration":2,"job_id":"9999999","created_at":"2016-07-26 08:36:49 UTC","updated_at":"2016-07-26 08:36:52 UTC","start_at":"2016-07-26 08:36:50 UTC","end_at":"2016-07-26 08:36:52 UTC","num_records":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobStatus, err := client.JobStatus("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Job Status is %s", jobStatus)
	if jobStatus != "killed" {
		t.Fatalf("Unexpected job status: %s", jobStatus)
	}
}

func TestJobStatusQueued(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"status":"queued","cpu_time":null,"result_size":null,"duration":2,"job_id":"9999999","created_at":"2016-07-26 08:36:49 UTC","updated_at":"2016-07-26 08:36:52 UTC","start_at":null,"end_at":null,"num_records":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobStatus, err := client.JobStatus("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Job Status is %s", jobStatus)
	if jobStatus != "queued" {
		t.Fatalf("Unexpected job status: %s", jobStatus)
	}
}

func TestJobStatusRunning(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"status":"running","cpu_time":null,"result_size":null,"duration":2,"job_id":"9999999","created_at":"2016-07-26 08:36:49 UTC","updated_at":"2016-07-26 08:36:52 UTC","start_at":"2016-07-26 08:36:50 UTC","end_at":null,"num_records":null}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobStatus, err := client.JobStatus("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Job Status is %s", jobStatus)
	if jobStatus != "running" {
		t.Fatalf("Unexpected job status: %s", jobStatus)
	}
}

func TestJobStatusSuccess(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"status":"success","cpu_time":null,"result_size":0,"duration":0,"job_id":"9999999","created_at":"2016-07-20 06:53:42 UTC","updated_at":"2016-07-20 06:53:43 UTC","start_at":"2016-07-20 06:53:43 UTC","end_at":"2016-07-20 06:53:43 UTC","num_records":10}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	jobStatus, err := client.JobStatus("9999999")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Job Status is %s", jobStatus)
	if jobStatus != "success" {
		t.Fatalf("Unexpected job status: %s", jobStatus)
	}
}

func TestSubmitHiveQuery(t *testing.T) {
	var dbName string
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"job":"9999999","job_id":"9999999","database":"sample_datasets"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	dbName = "sample_datasets"
	hiveQuery := Query{}
	hiveQuery.Type = "hive"
	hiveQuery.Query = "SELECT COUNT (*) FROM www_access"
	hiveJobID, err := client.SubmitQuery(dbName, hiveQuery)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Execute Job ID is %s", hiveJobID)
}

func TestSubmitPrestoQuery(t *testing.T) {
	var dbName string
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"job":"9999999","job_id":"9999999","database":"sample_datasets"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	dbName = "sample_datasets"
	prestoQuery := Query{}
	prestoQuery.Type = "presto"
	prestoQuery.Query = "SELECT COUNT (*) FROM www_access"
	prestoJobID, err := client.SubmitQuery(dbName, prestoQuery)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Execute Job ID is %s", prestoJobID)
}

func TestSubmitExportJob(t *testing.T) {
	var dbName string
	var tableName string
	var option map[string]string
	dbName = "sample_datasets"
	tableName = "www_access"
	option = map[string]string{
		"access_key_id":     "HOGEHOGEHOGEHOGE",
		"secret_access_key": "HOGEHOGEHOGEHOGEHOGE",
		"bucket":            "hoge-bucket",
		"file_format":       "json.gz",
	}
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"job":"9999999","job_id":"9999999","database":"sample_datasets"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	exportJobID, err := client.SubmitExportJob(dbName, tableName, "s3", option)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("Execute Job ID is %s", exportJobID)
}

func TestKillJob(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"job_id":"9999999","former_status":"running"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	killJobErr := client.KillJob("9999999")
	if killJobErr != nil {
		t.Fatalf("bad request: %s", killJobErr.Error())
	}
}
