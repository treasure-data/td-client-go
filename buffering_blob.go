package td_client

import (
	"bufio"
	"io"
)

type BufferingBlob struct {
	inner      Blob
}

type myReader struct {
	*bufio.Reader
	cl io.Closer
}

func (r *myReader) Close() error {
	return r.cl.Close()
}

func (blob *BufferingBlob) Reader() (io.ReadCloser, error) {
	rdr, err := blob.inner.Reader()
	if err != nil {
		return nil, err
	}
	return &myReader{ bufio.NewReader(rdr), rdr }, nil
}

func (blob *BufferingBlob) Size() (int64, error) {
	return blob.inner.Size()
}

func (blob *BufferingBlob) MD5Sum() ([]byte, error) {
	return blob.inner.MD5Sum()
}

func NewBufferingBlob(blob Blob) Blob {
	return &BufferingBlob{ blob }
}
