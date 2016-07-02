package magneticdb

import (
	"bytes"
	"io"
	"compress/gzip"
)

func compres(body []byte) ([]byte, error) {
	var b bytes.Buffer
	w, err := gzip.NewWriterLevel(&b, 9)
	defer w.Close()
	if err != nil {
		return []byte{}, err
	}
	w.Write(body)
	return b.Bytes(), nil
}

func decompres(body []byte) ([]byte, error) {
	var out bytes.Buffer
	b := bytes.NewReader(body)

	r, err := gzip.NewReader(b)
	if err != nil {
		return []byte{}, nil
	}
	defer r.Close()
	io.Copy(&out, r)
	return out.Bytes(), nil
}