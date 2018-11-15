package td_client

import "testing"

func TestShowDatabase(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(listDatabasesResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}

	// db found
	database, err := client.ShowDatabase("sample_datasets")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if database.Name != "sample_datasets" {
		t.Fatalf("database name mismatch: %s", database.Name)
	}

	// db not found
	database, err = client.ShowDatabase("not_found_db")
	if err == nil {
		t.Fatal("err expected")
	}
	if err.Error() != "Database 'not_found_db' does not exist" {
		t.Fatalf("unexpected err message: %s", err.Error())
	}
}

func TestListDatabases(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(listDatabasesResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	databaseList, err := client.ListDatabases()
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
	if len(*databaseList) != 3 {
		t.Fatal("not expected database count")
	}
}

func TestDeleteDatabase(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(deleteDatabasesResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.DeleteDatabase("test")
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

func TestCreateDatabase(t *testing.T) {
	client, err := NewTDClient(Settings{
		Transport: &DummyTransport{[]byte(createDatabasesResponse)},
	})
	if err != nil {
		t.Fatalf("failed create client: %s", err.Error())
	}
	err = client.CreateDatabase("test", nil)
	if err != nil {
		t.Fatalf("bad request: %s", err.Error())
	}
}

const listDatabasesResponse = `
{
   "databases":[
      {
         "name":"test",
         "created_at":"2016-11-07 09:37:37 UTC",
         "updated_at":"2016-11-07 09:37:37 UTC",
         "count":69664,
         "organization":null,
         "permission":"administrator",
         "delete_protected": true
      },
      {
         "name":"sample_datasets",
         "created_at":"2014-10-04 01:13:11 UTC",
         "updated_at":"2016-04-21 06:31:45 UTC",
         "count":8812278,
         "organization":null,
         "permission":"query_only",
         "delete_protected": false
      },
      {
         "name":"information_schema",
         "created_at":"2016-12-15 05:24:21 UTC",
         "updated_at":"2016-12-15 05:24:21 UTC",
         "count":0,
         "organization":null,
         "permission":"query_only",
         "delete_protected": false
      }
   ]
}
`

const deleteDatabasesResponse = `
{
   "database":"test"
}
`

const createDatabasesResponse = `
{
   "database":"test"
}
`
