//go:build ignore
// +build ignore

package td_client

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/ugorji/go/codec"
	"os"
	"strconv"
	"time"
)

func CompressWithGzip(b []byte) []byte {
	retval := bytes.Buffer{}
	w := gzip.NewWriter(&retval)
	w.Write(b)
	w.Close()
	return retval.Bytes()
}

func Example_walkthrough() {
	apiKey := os.Getenv("TD_CLIENT_API_KEY")
	client, err := NewTDClient(Settings{
		ApiKey: apiKey,
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	status, err := client.ServerStatus()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("status: %s\n", status.Status)
	account, err := client.ShowAccount()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("account:")
	fmt.Printf("  id: %d\n", account.Id)
	fmt.Printf("  plan: %d\n", account.Plan)
	fmt.Printf("  storageSize: %d\n", account.StorageSize)
	fmt.Printf("  guaranteedCores: %d\n", account.GuaranteedCores)
	fmt.Printf("  maximumCores: %d\n", account.MaximumCores)
	fmt.Printf("  createdAt: %s\n", account.CreatedAt.Format(time.RFC3339))
	results, err := client.ListResults()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("%d results\n", len(*results))
	databases, err := client.ListDatabases()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("%d databases\n", len(*databases))
	for _, database := range *databases {
		fmt.Printf("  name: %s\n", database.Name)
		tables, err := client.ListTables(database.Name)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("  %d tables\n", len(*tables))
		for _, table := range *tables {
			fmt.Printf("    name: %s\n", table.Name)
			fmt.Printf("    type: %s\n", table.Type)
			fmt.Printf("    count: %d\n", table.Count)
			fmt.Printf("    primaryKey: %s\n", table.PrimaryKey)
			fmt.Printf("    schema: %v\n", table.Schema)
		}
	}
	err = client.CreateDatabase("sample_db2", nil)
	if err != nil {
		_err := err.(*APIError)
		if _err == nil || _err.Type != AlreadyExistsError {
			fmt.Println(err.Error())
			return
		}
	}
	err = client.CreateLogTable("sample_db2", "test")
	if err != nil {
		_err := err.(*APIError)
		if _err == nil || _err.Type != AlreadyExistsError {
			fmt.Println(err.Error())
			return
		}
	} else {
		err = client.UpdateSchema("sample_db2", "test", []interface{}{
			[]string{"a", "string"},
			[]string{"b", "string"},
		})
	}
	data := bytes.Buffer{}
	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(&data, &handle)
	for i := 0; i < 10000; i += 1 {
		encoder.Encode(map[string]interface{}{
			"time": i, "a": strconv.Itoa(i), "b": strconv.Itoa(i),
		})
	}
	payload := CompressWithGzip(data.Bytes())
	fmt.Printf("payloadSize:%d\n", len(payload))
	time_, err := client.Import("sample_db2", "test", "msgpack.gz", (InMemoryBlob)(payload), "")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("elapsed time:%g\n", time_)
	jobId, err := client.SubmitQuery("sample_db2", Query{
		Type:       "hive",
		Query:      "SELECT COUNT(*) AS c FROM test WHERE a >= 5000",
		ResultUrl:  "",
		Priority:   0,
		RetryLimit: 0,
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("jobId:%s\n", jobId)
	for {
		status, err := client.JobStatus(jobId)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("jobStatus:%s\n", status)
		if status != "queued" {
			break
		}
		time.Sleep(1000000000)
	}
	{
		jobDesc, err := client.ShowJob(jobId)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("query:%s\n", jobDesc.Query)
		fmt.Printf("debug.cmdOut:%s\n", jobDesc.Debug.CmdOut)
		fmt.Printf("debug.stdErr:%s\n", jobDesc.Debug.StdErr)
		fmt.Printf("url:%s\n", jobDesc.Url)
		fmt.Printf("startAt:%s\n", jobDesc.StartAt.String())
		fmt.Printf("endAt:%s\n", jobDesc.EndAt.String())
		fmt.Printf("cpuTime:%g\n", jobDesc.CpuTime)
		fmt.Printf("resultSize:%d\n", jobDesc.ResultSize)
		fmt.Printf("priority:%d\n", jobDesc.Priority)
		fmt.Printf("hiveResultSchema:%v\n", jobDesc.HiveResultSchema)
	}
	for {
		time.Sleep(1000000000)
		status, err := client.JobStatus(jobId)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("jobStatus:%s\n", status)
		if status != "queued" && status != "running" {
			break
		}
	}
	{
		jobDesc, err := client.ShowJob(jobId)
		if err != nil {
			fmt.Println(err.Error())
			return
		}
		fmt.Printf("query:%s\n", jobDesc.Query)
		fmt.Printf("debug.cmdOut:%s\n", jobDesc.Debug.CmdOut)
		fmt.Printf("debug.stdErr:%s\n", jobDesc.Debug.StdErr)
		fmt.Printf("url:%s\n", jobDesc.Url)
		fmt.Printf("startAt:%s\n", jobDesc.StartAt.String())
		fmt.Printf("endAt:%s\n", jobDesc.EndAt.String())
		fmt.Printf("cpuTime:%g\n", jobDesc.CpuTime)
		fmt.Printf("resultSize:%d\n", jobDesc.ResultSize)
		fmt.Printf("priority:%d\n", jobDesc.Priority)
		fmt.Printf("hiveResultSchema:%v\n", jobDesc.HiveResultSchema)
	}
	err = client.JobResultEach(jobId, func(v interface{}) error {
		fmt.Printf("Result:%v\n", v)
		return nil
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	type_, err := client.DeleteTable("sample_db2", "test")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("deleteTable result: %s\n", type_)
	err = client.DeleteDatabase("sample_db2")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func ExampleTDClient_ServerStatus() {
	status, err := client.ServerStatus()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("status: %s\n", status.Status)
}

func ExampleTDClient_ShowAccount() {
	account, err := client.ShowAccount()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println("account:")
	fmt.Printf("id: %d\n", account.Id)
	fmt.Printf("plan: %d\n", account.Plan)
	fmt.Printf("storageSize: %d\n", account.StorageSize)
	fmt.Printf("guaranteedCores: %d\n", account.GuaranteedCores)
	fmt.Printf("maximumCores: %d\n", account.MaximumCores)
	fmt.Printf("createdAt: %s\n", account.CreatedAt.Format(time.RFC3339))
}

func ExampleTDClient_ListResults() {
	results, err := client.ListResults()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("%d results\n", len(*results))
}

func ExampleTDClient_ListDatabases() {
	databases, err := client.ListDatabases()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	for _, database := range *databases {
		fmt.Printf("name: %s\n", database.Name)
	}
}

func ExampleTDClient_ListTables() {
	tables, err := client.ListTables("sample_db")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("%d tables\n", len(*tables))
	for _, table := range *tables {
		fmt.Printf("name: %s\n", table.Name)
		fmt.Printf("type: %s\n", table.Type)
		fmt.Printf("count: %d\n", table.Count)
		fmt.Printf("primaryKey: %s\n", table.PrimaryKey)
		fmt.Printf("schema: %v\n", table.Schema)
	}
}

func ExampleTDClient_CreateDatabase() {
	err := client.CreateDatabase("sample_db2", nil)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func ExampleTDClient_CreateLogTable() {
	err := client.CreateLogTable("sample_db2", "test")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func ExampleTDClient_UpdateSchema() {
	err := client.UpdateSchema("sample_db2", "test", []interface{}{
		[]string{"a", "string"},
		[]string{"b", "string"},
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func ExampleTDClient_Import() {
	payload := []byte{ /*...*/ } // gzip'ed msgpack records
	time_, err := client.Import("sample_db2", "test", "msgpack.gz", (InMemoryBlob)(payload), "")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
}

func ExampleTDClient_SubmitQuery() {
	jobId, err := client.SubmitQuery("sample_db2", Query{
		Type:       "hive",
		Query:      "SELECT COUNT(*) AS c FROM test WHERE a >= 5000",
		ResultUrl:  "",
		Priority:   0,
		RetryLimit: 0,
	})
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("jobId:%s\n", jobId)
}

func ExampleTDClient_ShowJob() {
	jobDesc, err := client.ShowJob(jobId)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Printf("query:%s\n", jobDesc.Query)
	fmt.Printf("debug.cmdOut:%s\n", jobDesc.Debug.CmdOut)
	fmt.Printf("debug.stdErr:%s\n", jobDesc.Debug.StdErr)
	fmt.Printf("url:%s\n", jobDesc.Url)
	fmt.Printf("startAt:%s\n", jobDesc.StartAt.String())
	fmt.Printf("endAt:%s\n", jobDesc.EndAt.String())
	fmt.Printf("cpuTime:%g\n", jobDesc.CpuTime)
	fmt.Printf("resultSize:%d\n", jobDesc.ResultSize)
	fmt.Printf("priority:%d\n", jobDesc.Priority)
	fmt.Printf("hiveResultSchema:%v\n", jobDesc.HiveResultSchema)
}

func ExampleTDClient_JobResult() {
	err := client.JobResultEach(jobId, func(v interface{}) error {
		fmt.Printf("Result:%v\n", v)
		return nil
	})
}
