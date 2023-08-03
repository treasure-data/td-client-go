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

import "testing"

func TestCreateBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	createResult, err := client.CreateBulkImport("test_bulk_import_name", "test_bulk_import_db", "test_bulk_import_table", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", createResult)
}

func TestDeleteBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.DeleteBulkImport("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestShowBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","status":"uploading","job_id":null,"valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"upload_frozen":false,"database":"test_bulk_import_db","table":"test_bulk_import_table"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	showResult, err := client.ShowBulkImport("test-0000")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", showResult)
}

func TestListBulkImports(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"bulk_imports":[{"name":"test_bulk_import_name1","status":"uploading","job_id":null,"valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"upload_frozen":false,"database":"test_bulk_import_db","table":"test_bulk_import_table"},{"name":"test_bulk_import_name2","status":"uploading","job_id":null,"valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"upload_frozen":false,"database":null,"table":null},{"name":"test_bulk_import_name3","status":"uploading","job_id":null,"valid_records":null,"error_records":null,"valid_parts":null,"error_parts":null,"upload_frozen":false,"database":"test_bulk_import_db","table":null}]}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	listResult, err := client.ListBulkImports(nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", listResult)
}

func TestListBulkImportParts(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"parts":["test_bulk_import_part_1", "test_bulk_import_part_2"],"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	listPartsResult, err := client.ListBulkImportParts("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", listPartsResult)
}

func TestUploadBulkImportPart(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	buffer := BufferingBlob{}
	uploadPartResult, err := client.UploadBulkImportPart("test_bulk_import_name", "test_bulk_import_part_1", buffer.inner)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", uploadPartResult)
}

func TestDeleteBulkImportPart(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.DeleteBulkImportPart("test_bulk_import_name", "test_bulk_import_part_1", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestFreezeBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	freezeResult, err := client.FreezeBulkImport("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", freezeResult)
}

func TestUnfreezeBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	unFreezeResult, err := client.UnfreezeBulkImport("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", unFreezeResult)
}

func TestPerformBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name", "job_id": 11111111111}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	performResult, err := client.PerformBulkImport("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if performResult.JobID != "11111111111" {
		t.Fatalf("want job ID 11111111111, got %q", performResult.JobID)
	}
	t.Logf("%+v", performResult)
}

func TestCommitBulkImport(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(`{"name":"test_bulk_import_name","bulk_import":"test_bulk_import_name"}`)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	commitResult, err := client.CommitBulkImport("test_bulk_import_name", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	t.Logf("%+v", commitResult)
}
