package td_client

type ServerStatusResult struct {
	Status string
}


var serverStatusSchema = map[string]interface{} {
	"status": "",
}

func (client *TDClient) ServerStatus() (*ServerStatusResult, error) {
	resp, err := client.get("/v3/system/server_status", nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, client.buildError(resp, -1, "Server is down", nil)
	}
	js, err := client.checkedJson(resp, serverStatusSchema)
	if err != nil {
		return nil, err
	}
	return &ServerStatusResult {
		Status: js["status"].(string),
	}, nil
}
