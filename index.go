package magneticdb

import (
	"errors"
	"sort"
	"github.com/google/btree"
)


var (
	errIndexNotFound = errors.New("Index not found")
	defaultdegree = 128
)

type Index struct {
	indexies map[string]*IndexEntry
	tree     *btree.BTree
}

func NewIndex() *Index {
	return &Index{
		indexies: map[string]*IndexEntry{},
		tree:     btree.New(defaultdegree),
	}
}

// CreateIndex provides new index
func (idx *Index) CreateIndex(title string){
	idx.indexies[title] = &IndexEntry{}
}

func (idx *Index) Put(title []byte) {
	idx.tree.ReplaceOrInsert(&IndexEntry{Key: title})
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

// List returns list of the available indexes
func (idx *Index) List() []string {
	idxes := make([]string, 0, len(idx.indexies))
	for key, _ := range idx.indexies {
		idxes = append(idxes, key)
	}

	sort.Strings(idxes)
	return idxes
}

func (idx *Index) Equal(id int) {

}
