Treasure Data API client for Go
===============================

[![Build Status](https://travis-ci.org/treasure-data/td-client-go.svg?branch=master)](https://travis-ci.org/treasure-data/td-client-go)

Build
-----
- make
-- To build all the code and run all the tests
- make test-verbose
-- To run tests verbosely
- make help
-- description of targets
- make clean
-- To cleanup locally

Synopsis
--------

```go
package main

import (
	td_client "github.com/treasure-data/td-client-go"
)

func main() {
	client, err := td_client.NewTDClient(td_client.Settings {
		ApiKey: "YOUR-API-KEY-HERE",
	})
	if err != nil { ... }
	jobId, err := client.SubmitQuery("mydatabase", td_client.Query {
		Type: "hive",
		Query: "SELECT * FROM mytable WHERE value >= 500",
	})
	if err != nil { ... }
	for {
		status, err := client.JobStatus(jobId)
		if err != nil { ... }
		if status != "queued" && status != "running" { break }
		time.Sleep(1000000000)
	}
	err = client.JobResultEach(jobId, func(v interface{}) error {
		fmt.Printf("Result:%v\n", v)
		return nil
	})
	if err != nil { ... }
}
```

License
-------

Copyright (C) 2014 Treasure Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
