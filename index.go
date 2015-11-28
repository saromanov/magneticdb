package magneticdb

import (
	"errors"
	"github.com/google/btree"
)

type IndexEntry struct {
	Idx       string
	Tablename string
	Modify    bool
}

var (
	errIndexNotFound = errors.New("Index not found")
)

type Index struct {
	indexies map[string]*IndexEntry
	tree     *btree.BTree
}

func NewIndex() *Index {
	return &Index{
		indexies: map[string]*IndexEntry{},
		tree:     btree.New(128),
	}
}

func (idx *Index) FindIndex(title string) (*IndexEntry, error) {
	for keyidx, value := range idx.indexies {
		if keyidx == title {
			return value, nil
		}
	}

	return nil, errIndexNotFound
}

func (idx *Index) DropIndex(title string) error {
	_, err := idx.FindIndex(title)
	if err != nil {
		return err
	}
	delete(idx.indexies, title)
	return nil
}

func (idx *Index) Equal(id int) {

}
