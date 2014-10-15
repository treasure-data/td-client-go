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

/*
Treasure Data API client.  The following API functions are covered at the
moment.

Account API:

	ShowAccount
	AccountCoreUtilization

Status API:

	ServerStatus

Database/Table API:

	ListDatabases
	DeleteDatabase
	CreateDatabase
	ListTables
	CreateTable
	DeleteTable
	SwapTable
	UpdateSchema
	UpdateExpire
	Tail

Job/Query API:

	ListJobs
	ShowJob
	KillJob
	SubmitQuery
	SubmitExportJob
	SubmitPartialDeleteJob
	JobStatus
	JobResult
	ListResults
	CreateResult
	DeleteResult

Import API:

	Import
*/
package td_client
