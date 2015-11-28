package magneticdb

import (
	"bytes"
	"github.com/google/btree"
)

type IndexEntry struct {
	Key       []byte
	Idx       string
	Tablename string
	Modify    bool
}

func (ie *IndexEntry) Less(item btree.Item) bool {
	return bytes.Compare(ie.Key, item.(*IndexEntry).Key) == -1
}
