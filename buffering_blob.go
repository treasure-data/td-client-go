package td_client

import (
	"bufio"
	"io"
)

type BufferingBlob struct {
	inner      Blob
	size       int
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
	return &myReader{ bufio.NewReaderSize(rdr, blob.size), rdr }, nil
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
	return &BufferingBlob{ blob, size }
}
