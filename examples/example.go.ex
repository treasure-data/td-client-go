package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"github.com/ugorji/go/codec"
	"os"
	"strconv"
	"time"
	td_client "github.com/treasure-data/td-client-go"
)

type Conf struct {
	DbName string
	TableName string
	BulkName string
	PartName []string
}

var conf = Conf{"sample_db2","test", "import", []string{"part1","part2"}}

func CompressWithGzip(b []byte) []byte {
	retval := bytes.Buffer{}
	w := gzip.NewWriter( &retval )
	w.Write( b )
	w.Close()
	return retval.Bytes()
}

func errHandler( params ...interface{} ) {

	var client *td_client.TDClient

	for i,p := range params {
		switch i {
			case 0:
				fmt.Println(p.(error).Error())
			case 1:
				client = p.(*td_client.TDClient)
			default:
				switch p.(string) {
					case "database":
						if err := client.DeleteDatabase( conf.DbName ); err != nil {
							fmt.Println( "DeleteDatabase error:", err.Error() )
						}
					case "table":
						if _, err := client.DeleteTable( conf.DbName, conf.TableName ); err != nil {
							fmt.Println( "DeleteTable error:", err.Error() )
						}
					case "bulk":
						if err := client.DeleteBulkImport( conf.BulkName, map[string]string{} ); err != nil {
							fmt.Println( "DeleteBulkImport error:", err.Error() )
						}
				}
		}
	}

	os.Exit( -1 )
}

func waitForJob( client *td_client.TDClient, jobId string ) {
	//
	// Wait for the job to be queued
	//

	for {
		if status, err := client.JobStatus( jobId ); err != nil {
			errHandler( err, client, "bulk", "table", "database" )
		} else {
			fmt.Println("jobStatus:", status)
			if status != "queued" {
				if desc, err := client.ShowJob( jobId ); err != nil {
					errHandler( err, client, "bulk", "table", "database" )
				} else {
					fmt.Println("query:", desc.Query)
					fmt.Println("debug.cmdOut:", desc.Debug.CmdOut)
					fmt.Println("debug.stdErr:", desc.Debug.StdErr)
					fmt.Println("url:", desc.Url)
					fmt.Println("startAt:", desc.StartAt.String())
					fmt.Println("endAt:", desc.EndAt.String())
					fmt.Println("cpuTime:", desc.CpuTime)
					fmt.Println("resultSize:", desc.ResultSize)
					fmt.Println("priority:", desc.Priority)
				}
				break
			}
			time.Sleep(1000000000)
		}
	}

	//
	// Wait for job to be completed
	//

	for {
		time.Sleep(1000000000)
		if status, err := client.JobStatus( jobId ); err != nil {
			errHandler( err, client, "bulk", "table", "database" )
		} else {
			fmt.Println("jobStatus:", status)
			if status != "queued" && status != "running" {
				if desc, err := client.ShowJob( jobId ); err != nil {
					errHandler( err, client, "bulk", "table", "database" )
				} else {
					fmt.Println("query:", desc.Query)
					fmt.Println("debug.cmdOut:", desc.Debug.CmdOut)
					fmt.Println("debug.stdErr:", desc.Debug.StdErr)
					fmt.Println("url:", desc.Url)
					fmt.Println("startAt:", desc.StartAt.String())
					fmt.Println("endAt:", desc.EndAt.String())
					fmt.Println("cpuTime:", desc.CpuTime)
					fmt.Println("resultSize:", desc.ResultSize)
					fmt.Println("priority:", desc.Priority)
				}
				break
			}
		}
	}
}

func main() {

	apiKey := os.Getenv("TD_CLIENT_API_KEY")

	var router = td_client.V3EndpointRouter{
		DefaultEndpoint: os.Getenv("TD_API_ENDPOINT"),
		ImportEndpoint:  os.Getenv("TD_API_IMPORT"),
	}

	client, err := td_client.NewTDClient( td_client.Settings {
		ApiKey: apiKey,
		Router: &router,
	} )
	if err != nil {
		errHandler( err )
	}

	//
	// Check the server status
	//

	if status, err := client.ServerStatus(); err != nil {
		errHandler( err )
	} else {
		fmt.Println("status:", status.Status)
	}

	//
	// Show details of the account
	//

	if account, err := client.ShowAccount(); err != nil {
		errHandler( err )
	} else {
		fmt.Println("account:")
		fmt.Println("  id:", account.Id)
		fmt.Println("  plan:", account.Plan)
		fmt.Println("  storageSize:", account.StorageSize)
		fmt.Println("  guaranteedCores:", account.GuaranteedCores)
		fmt.Println("  maximumCores:", account.MaximumCores)
		fmt.Println("  createdAt:", account.CreatedAt.Format(time.RFC3339))
		fmt.Println("  encryptStartAt:", account.EncryptStartAt.Format(time.RFC3339))
	}

	//
	// Fetch a list of all the databases for the account
	//

	if databases, err := client.ListDatabases(); err != nil {
		errHandler( err )
	} else {
		fmt.Println( len(*databases), " databases")

		//
		// Show table details for each database
		//

		for _, database := range *databases {
			fmt.Println("  name:", database.Name )
			if tables, err := client.ListTables(database.Name); err != nil {
				errHandler( err )
			} else {
				fmt.Println( "  ", len(*tables), " tables")
				for _, table := range *tables {
					fmt.Println("    table name:", table.Name)
					fmt.Println("    type:", table.Type)
					fmt.Println("    count:", table.Count)
					fmt.Println("    primaryKey:", table.PrimaryKey)
					fmt.Println("    schema:", table.Schema)
				}
			}
		}
	}

	//
	// Create a database to perform some example operations on
	//

	if err := client.CreateDatabase( conf.DbName, nil ); err != nil {
		_err := err.(*td_client.APIError)
		if _err == nil || _err.Type != td_client.AlreadyExistsError {
			errHandler( err )
		}
	} else {
		fmt.Println("Created datatbase:", conf.DbName)
	}

	//
	// Create a table in the database
	//

	if err := client.CreateLogTable(conf.DbName, conf.TableName); err != nil {
		_err := err.(*td_client.APIError)
		if _err == nil || _err.Type != td_client.AlreadyExistsError {
			errHandler( err, client, "database" )
		}
	} else {
		fmt.Println("Created table:", conf.TableName)
	}

	//
	// Add/update a schema to the newly created table
	//

	schema := []interface{}{[]string{"a", "string"}, []string{"b", "string"}}

	if err := client.UpdateSchema( conf.DbName, conf.TableName, schema ); err != nil {
		errHandler( err, client, "table", "database" )
	} else {
		fmt.Println("Updated schema:", schema)
	}

	//
	// Generate some data to upload
	//

	data := bytes.Buffer{}
	handle := codec.MsgpackHandle{}
	encoder := codec.NewEncoder(&data, &handle)
	for i := 0; i < 10000; i += 1 {
		encoder.Encode(map[string]interface{}{
			"time": i, "a": strconv.Itoa(i), "b": strconv.Itoa(i),
		})
	}

	var payload td_client.InMemoryBlob

	payload = CompressWithGzip( data.Bytes() )

	//
	// Perform a simple (one payload) import
	//

	if ticks, err := client.Import( conf.DbName, conf.TableName, "msgpack.gz", payload, "" ); err != nil {
		errHandler( err, client, "table", "database" )
	} else {
		fmt.Println("elapsed time:%g", ticks)
	}

	//
	// Create a bulk import for importing multiple payloads
	//

	if _, err = client.CreateBulkImport( conf.BulkName, conf.DbName, conf.TableName, map[string]string{} ); err != nil {
		_err := err.(*td_client.APIError)
		if _err == nil || _err.Type != td_client.AlreadyExistsError {
			errHandler( err, client, "table", "database" )
		}
	} else {
		fmt.Println("Created bulk import:", conf.BulkName)
	}

	//
	// Upload the first part
	//

	if part, err := client.UploadBulkImportPart( conf.BulkName, conf.PartName[0], payload ); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	} else {
		fmt.Println("Upload bulk import part:", part)
	}

	//
	// Upload a second part
	//

	if part, err := client.UploadBulkImportPart( conf.BulkName, conf.PartName[1], payload ); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	} else {
		fmt.Println("Upload bulk import part:", part)
	}

	//
	// Can continue to upload more parts here
	//

	//
	// Display some info about the bulk import
	//

	if elements, err := client.ListBulkImports( map[string]string{} ); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	} else {
		for _, v := range *elements {
			if element, err := client.ShowBulkImport( v.Name ); err != nil {
				errHandler( err, client, "bulk", "table", "database" )
			} else {
				fmt.Println("Show bulk import:", element)
			}

			if parts, err := client.ListBulkImportParts( v.Name, map[string]string{} ); err != nil {
				errHandler( err, client, "bulk", "table", "database" )
			} else {
				fmt.Println("List bulk import parts:", parts)
			}
		}
	}

	//
	// Tell the API to prepare/ingest the data
	//

	var jobId string

	if perform, err := client.PerformBulkImport( conf.BulkName, map[string]string{} ); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	} else {
		fmt.Println("Perform bulk import:", perform)
		jobId = perform.JobID
	}

	//
	// Wait for the import work to be completed
	//

	waitForJob( client, jobId )

	//
	// Commit the imported data to the database
	//

	if commit, err := client.CommitBulkImport( conf.BulkName, map[string]string{} ); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	} else {
		fmt.Println("Commit bulk import:", commit)
	}

	//
	// Clean up the bulk import
	//

	if err := client.DeleteBulkImport( conf.BulkName, map[string]string{} ); err != nil {
		errHandler( err, client, "table", "database" )
	}

	//
	// Run a query to check that all of the data was committed
	//

	if jobId, err = client.SubmitQuery(conf.DbName, td_client.Query{
		Type:       "hive",
		Query:      "SELECT COUNT(*) AS c FROM " + conf.TableName,
		ResultUrl:  "",
		Priority:   0,
		RetryLimit: 0,
	}); err != nil {
		errHandler( err, client, "bulk", "table", "database" )
	}

	//
	// Wait for the query to be completed
	//

	waitForJob( client, jobId )

	if err := client.JobResultEach(jobId, func(v interface{}) error {
		fmt.Println("Job result:", v)
		return nil
	}); err != nil {
		errHandler( err, client, "table", "database" )
	}

	//
	// Clean up the table that was created
	//

	if table, err := client.DeleteTable( conf.DbName, conf.TableName ); err != nil {
		errHandler( err, client, "database" )
	} else {
		fmt.Println("Delete table:", table )
	}

	//
	// Clean up the database that was created
	//

	if err := client.DeleteDatabase( conf.DbName ); err != nil {
		errHandler( err )
	} else {
		fmt.Println("Delete database:", conf.DbName)
	}
}
