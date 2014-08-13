package td_client

import (
	"fmt"
	"net/url"
)

type ListResultsResultElement struct {
	Name string
	Url string
}

type ListResultsResult []ListResultsResultElement

var listResultsSchema = map[string]interface{} {
	"results": []map[string]string {
		{
			"name":"",
			"url":"",
		},
	},
}

func (client *TDClient) ListResults() (*ListResultsResult, error) {
	resp, err := client.get("/v3/result/list", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "List result tables failed", nil)
	}
	js, err := client.checkedJson(resp, listResultsSchema)
	if err != nil {
		return nil, err
	}
	results := js["results"].([]map[string]string)
	retval := make(ListResultsResult, len(results))
	for i, v := range results {
		retval[i] = ListResultsResultElement {
			Name: v["name"],
			Url: v["url"],
		}
	}
	return &retval, nil
}

func (client *TDClient) CreateResult(name, url_ string) error {
	resp, err := client.post(fmt.Sprintf("/v3/result/create/%s", url.QueryEscape(name)), url.Values { "url": { url_ } })
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Create result table failed", nil)
	}
	return nil
}


func (client *TDClient) DeleteResult(name string) error {
	resp, err := client.post(fmt.Sprintf("/v3/result/delete/%s", url.QueryEscape(name)), nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return client.buildError(resp, -1, "Delete result table failed", nil)
	}
	return nil
}
