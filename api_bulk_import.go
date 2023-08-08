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
)

type BulkImportResult struct {
	Name       string
	BulkImport string
}

var bulkImportResultSchema = map[string]interface{}{
	"name":        "",
	"bulk_import": "",
}

type BulkImportElement struct {
	Name         string
	Database     string
	Table        string
	Status       string
	JobID        string
	ValidRecords int
	ErrorRecords int
	ValidParts   int
	ErrorParts   int
	UploadFrozen bool
}

type ListBulkImportElements []BulkImportElement

var bulkImportElementSchema = map[string]interface{}{
	"name":          "",
	"database":      Optional{"", nil},
	"table":         Optional{"", nil},
	"status":        Optional{"", nil},
	"job_id":        Optional{"", nil},
	"upload_frozen": false,
	"valid_records": Optional{0, 0},
	"error_records": Optional{0, 0},
	"valid_parts":   Optional{0, 0},
	"error_parts":   Optional{0, 0},
}

var listBulkImportElementsSchema = map[string]interface{}{
	"bulk_imports": []map[string]interface{}{
		{
			"name":          "",
			"database":      Optional{"", nil},
			"table":         Optional{"", nil},
			"status":        Optional{"", nil},
			"job_id":        Optional{"", nil},
			"upload_frozen": false,
			"valid_records": Optional{0, 0},
			"error_records": Optional{0, 0},
			"valid_parts":   Optional{0, 0},
			"error_parts":   Optional{0, 0},
		},
	},
}

type ListBulkImportParts struct {
	Name       string
	BulkImport string
	Parts      []string
}

var listBulkImportPartsSchema = map[string]interface{}{
	"name":        "",
	"bulk_import": "",
	"parts":       []string{},
}

type PerformBulkImportResult struct {
	Name       string
	BulkImport string
	JobID      string
}

var performBulkImportResultSchema = map[string]interface{}{
	"name":        "",
	"bulk_import": "",
	"job_id":      int64(0),
}

func (client *TDClient) CreateBulkImport(name string, db string, table string, options map[string]string) (*BulkImportResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/create/%s/%s/%s", url.QueryEscape(name), url.QueryEscape(db), url.QueryEscape(table)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for create bulk import", nil)
	}
	js, err := client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
	}, nil
}

func (client *TDClient) DeleteBulkImport(name string, options map[string]string) error {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/delete/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Failed for delete bulk import", nil)
	}
	_, err = client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return err
	}
	return nil
}

func (client *TDClient) ShowBulkImport(name string) (*BulkImportElement, error) {
	resp, err := client.get(fmt.Sprintf("/v3/bulk_import/show/%s", url.QueryEscape(name)), nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for show bulk import", nil)
	}
	js, err := client.checkedJson(resp, bulkImportElementSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportElement{
		Name:         js["name"].(string),
		Database:     js["database"].(string),
		Table:        js["table"].(string),
		JobID:        js["job_id"].(string),
		Status:       js["status"].(string),
		UploadFrozen: js["upload_frozen"].(bool),
		ValidRecords: js["valid_records"].(int),
		ErrorRecords: js["error_records"].(int),
		ValidParts:   js["valid_parts"].(int),
		ErrorParts:   js["error_parts"].(int),
	}, nil

}

func (client *TDClient) ListBulkImports(options map[string]string) (*ListBulkImportElements, error) {
	resp, err := client.get("/v3/bulk_import/list", dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for show list bulk imports", nil)
	}
	js, err := client.checkedJson(resp, listBulkImportElementsSchema)
	if err != nil {
		return nil, err
	}
	bulkImports := js["bulk_imports"].([]map[string]interface{})
	listBulkImportElements := make(ListBulkImportElements, len(bulkImports))
	for i, v := range bulkImports {
		listBulkImportElements[i] = BulkImportElement{
			Name:         v["name"].(string),
			Database:     v["database"].(string),
			Table:        v["table"].(string),
			JobID:        v["job_id"].(string),
			Status:       v["status"].(string),
			UploadFrozen: v["upload_frozen"].(bool),
			ValidRecords: v["valid_records"].(int),
			ErrorRecords: v["error_records"].(int),
			ValidParts:   v["valid_parts"].(int),
			ErrorParts:   v["error_parts"].(int),
		}
	}
	return &listBulkImportElements, nil
}

func (client *TDClient) ListBulkImportParts(name string, options map[string]string) (*ListBulkImportParts, error) {
	resp, err := client.get(fmt.Sprintf("/v3/bulk_import/list_parts/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for show list bulk import parts", nil)
	}
	js, err := client.checkedJson(resp, listBulkImportPartsSchema)
	if err != nil {
		return nil, err
	}
	parts, _ := js["parts"].([]string)
	return &ListBulkImportParts{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
		Parts:      parts,
	}, nil
}

func (client *TDClient) UploadBulkImportPart(name string, part_name string, blob Blob) (*BulkImportResult, error) {
	resp, err := client.put(fmt.Sprintf("/v3/bulk_import/upload_part/%s/%s", url.QueryEscape(name), url.QueryEscape(part_name)), blob)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for upload bulk import part", nil)
	}
	js, err := client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
	}, nil
}

func (client *TDClient) DeleteBulkImportPart(name string, part_name string, options map[string]string) error {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/delete_part/%s/%s", url.QueryEscape(name), url.QueryEscape(part_name)), dictToValues(options))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Failed for delete bulk import part", nil)
	}
	_, err = client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return err
	}
	return nil
}

func (client *TDClient) FreezeBulkImport(name string, options map[string]string) (*BulkImportResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/freeze/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for freeze bulk import", nil)
	}
	js, err := client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
	}, nil
}

func (client *TDClient) UnfreezeBulkImport(name string, options map[string]string) (*BulkImportResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/unfreeze/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for unfreeze bulk import", nil)
	}
	js, err := client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
	}, nil
}

func (client *TDClient) PerformBulkImport(name string, options map[string]string) (*PerformBulkImportResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/perform/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for perform bulk import", nil)
	}
	js, err := client.checkedJson(resp, performBulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &PerformBulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
		JobID:      strconv.FormatInt(js["job_id"].(int64), 10),
	}, nil
}

func (client *TDClient) CommitBulkImport(name string, options map[string]string) (*BulkImportResult, error) {
	resp, err := client.post(fmt.Sprintf("/v3/bulk_import/commit/%s", url.QueryEscape(name)), dictToValues(options))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Failed for commit bulk import", nil)
	}
	js, err := client.checkedJson(resp, bulkImportResultSchema)
	if err != nil {
		return nil, err
	}
	return &BulkImportResult{
		Name:       js["name"].(string),
		BulkImport: js["bulk_import"].(string),
	}, nil
}
