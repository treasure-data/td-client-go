package td_client

import "testing"

func TestShowTable(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(showTableResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	table, err := client.ShowTable("test_database", "test_table")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if table.Name != "test_table" {
		t.Fatal("unexpected table name")
	}
	t.Log(table)
}

func TestListTables(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(listTablesResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	tableList, err := client.ListTables("test")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(*tableList) != 3 {
		t.Fatal("not expected database count")
	}
}

func TestCreateLogTable(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(createLogTableResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.CreateLogTable("test_database", "test_table")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestSwapTable(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(swapTableResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.SwapTable("test_database", "test_table2", "test_table")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}

}

func TestUpdateTable(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(updateTableResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.UpdateTable("test_database", "test_table", make(map[string]string))
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestUpdateSchema(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(updateSchemaResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.UpdateSchema("test_database", "test_table", updateSchemaRequestParams)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}

}

func TestUpdateExpire(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(updateExpireResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.UpdateExpire("test_database", "test_table", 10)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestDeleteTable(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(deleteTableResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	_type, err := client.DeleteTable("test_database", "test_table")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if _type != "log" {
		t.Fatalf("unexpected type: %s", _type)
	}
}

func TestTail(t *testing.T) {}

const showTableResponse = `
{
   "id":999999,
   "name":"test_table",
   "estimated_storage_size":0,
   "counter_updated_at":null,
   "last_log_timestamp":null,
   "delete_protected":false,
   "created_at":"2017-05-14 12:19:37 UTC",
   "updated_at":"2017-05-14 15:53:17 UTC",
   "type":"log",
   "count":0,
   "schema":"[[\"col1\",\"string\"]]",
   "expire_days":10,
   "include_v":true
}
`

const listTablesResponse = `
{
   "database":"nh4_test",
   "tables":[
      {
         "id":99999991,
         "name":"test_table_1",
         "estimated_storage_size":67158,
         "counter_updated_at":"2017-05-13T11:39:53Z",
         "last_log_timestamp":"2017-05-07T08:59:35Z",
         "delete_protected":false,
         "created_at":"2017-03-30 09:00:14 UTC",
         "updated_at":"2017-05-07 08:59:40 UTC",
         "type":"log",
         "count":1751,
         "schema":"[[\"id\",\"long\"],[\"fizz\",\"string\"],[\"buzz\",\"string\"],[\"fizzbuzz\",\"string\"],[\"created_at\",\"string\"],[\"created_by\",\"long\"],[\"updated_at\",\"string\"],[\"updated_by\",\"long\"],[\"deleted_at\",\"string\"],[\"deleted_by\",\"long\"]]",
         "expire_days":null,
         "include_v":true
      },
      {
         "id":99999992,
         "name":"test_table_2",
         "estimated_storage_size":84251,
         "counter_updated_at":"2017-05-13T11:39:53Z",
         "last_log_timestamp":"2016-12-28T20:59:52Z",
         "delete_protected":false,
         "created_at":"2016-11-07 11:57:15 UTC",
         "updated_at":"2017-05-07 08:52:15 UTC",
         "type":"log",
         "count":2539,
         "schema":"[[\"id\",\"long\"],[\"hogehoge\",\"string\"],[\"fugafuga\",\"long\"],[\"created_at\",\"string\"],[\"updated_at\",\"string\"],[\"created_by\",\"long\"],[\"updated_by\",\"long\"]]",
         "expire_days":null,
         "include_v":true
      },
      {
         "id":99999993,
         "name":"test_table_3",
         "estimated_storage_size":1609,
         "counter_updated_at":"2017-05-13T11:39:53Z",
         "last_log_timestamp":"2017-05-07T08:52:24Z",
         "delete_protected":false,
         "created_at":"2016-11-07 11:56:58 UTC",
         "updated_at":"2017-05-07 08:52:27 UTC",
         "type":"log",
         "count":76,
         "schema":"[[\"id\",\"long\"],[\"foo\",\"string\"],[\"var\",\"string\"],[\"created_at\",\"string\"],[\"created_by\",\"long\"],[\"updated_at\",\"string\"],[\"updated_by\",\"long\"],[\"deleted_at\",\"string\"],[\"deleted_by\",\"long\"]]",
         "expire_days":null,
         "include_v":true
      }
   ]
}
`

const createLogTableResponse = `
{
	"table":"test_table",
	"type":"log",
	"database":"test_database"
}
`

const swapTableResponse = `
{
	"database":"test_database",
	"table1":"test_table2",
	"table2":"test_table"
}
`

var updateSchemaRequestParams = []interface{}{
	[]interface{}{"col1", "string"},
	[]interface{}{"col2", "long"},
	[]interface{}{"col3", "string"},
}

const updateSchemaResponse = `
{
	"table":"test_table",
	"type":"log",
	"database":"test_database"
}
`
const updateExpireResponse = `
{
	"table":"test_table",
	"type":"log",
	"database":"test_database"
}
`

const updateTableResponse = `
{
	"table":"test_table",
	"type":"log",
	"database":"test_database"
}
`

const deleteTableResponse = `
{
	"table":"test_table",
	"type":"log",
	"database":"test_database"
}
`
