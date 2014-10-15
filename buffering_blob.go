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
	"bufio"
	"io"
)

// BufferingBlob wraps the other blob so Reader() would return the buffered reader.
// This is helpful if the blob is backed by a *os.File.
type BufferingBlob struct {
	inner Blob
	size  int
}

type myReader struct {
	*bufio.Reader
	cl io.Closer
}

const defaultBufferSize = 4096

func (r *myReader) Close() error {
	return r.cl.Close()
}

func (blob *BufferingBlob) Reader() (io.ReadCloser, error) {
	rdr, err := blob.inner.Reader()
	if err != nil {
		return nil, err
	}
	return &myReader{bufio.NewReaderSize(rdr, blob.size), rdr}, nil
}

func (blob *BufferingBlob) Size() (int64, error) {
	return blob.inner.Size()
}

func (blob *BufferingBlob) MD5Sum() ([]byte, error) {
	return blob.inner.MD5Sum()
}

func NewBufferingBlob(blob Blob) Blob {
	return NewBufferingBlobSize(blob, defaultBufferSize)
}

func NewBufferingBlobSize(blob Blob, size int) Blob {
	return &BufferingBlob{blob, size}
}
