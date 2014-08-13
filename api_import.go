package td_client

import (
	"fmt"
	"bytes"
	"errors"
	"net/url"
	"encoding/hex"
)

var importSchema = map[string]interface{} {
	"unique_id": Optional{"", ""},
	"database": "",
	"table": "",
	"md5_hex": "",
	"elapsed_time": 0.,
}

func (client *TDClient) Import(db string, table string, format string, blob Blob, uniqueId string) (float64, error) {
	requestUri := ""
	if uniqueId != "" {
		requestUri = fmt.Sprintf(
			"/v3/table/import_with_id/%s/%s/%s/%s",
			url.QueryEscape(db),
			url.QueryEscape(table),
			url.QueryEscape(uniqueId),
			url.QueryEscape(format),
		)
	} else {
		requestUri = fmt.Sprintf(
			"/v3/table/import/%s/%s/%s",
			url.QueryEscape(db),
			url.QueryEscape(table),
			url.QueryEscape(format),
		)
	}
	resp, err := client.put(requestUri, blob)
	if err != nil {
		return 0., err
	}
	defer resp.Body.Close()
	if resp.StatusCode / 100 != 2 {
		return 0., client.buildError(resp, -1, "Import failed", nil)
	}
	js, err := client.checkedJson(resp, importSchema)
	if err != nil {
		return 0., err
	}
	expectedMD5Sum, err := blob.MD5Sum()
	if err == nil {
		md5Hex := js["md5_hex"].(string)
		if md5Hex != "" {
			md5Sum, err := hex.DecodeString(md5Hex)
			if err != nil {
				return 0., err
			}
			if bytes.Compare(md5Sum, expectedMD5Sum) != 0 {
				return 0., errors.New("Checksum mismatch")
			}
		}
	}
	time_ := js["elapsed_time"].(float64)
	return time_, nil
}
