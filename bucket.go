package magneticdb

import (
	"errors"
	"reflect"
)

type Bucket struct {
	items map[string][]*Item
}

var (
	errBucketIsNotExist = errors.New("Bucket is not exist")
	errKeyIsNotFound    = errors.New("Key is not found")
)

// CreateBucket provides creational of the new bucket
func (b *Bucket) CreateBucket(title string) {
	b.items[title] = []*Item{}
}

func (b *Bucket) SetToBucket(title string, key, value []byte) error {
	_, ok := b.items[title]
	if !ok {
		return errBucketIsNotExist
	}

	newitem, err := set(title, key, value)
	if err != nil {
		return err
	}
	b.items[title] = append(b.items[title], newitem)
	return nil
}

func (b *Bucket) GetFromBucket(title string, key []byte) ([]byte, error) {
	items, ok := b.items[title]
	if !ok {
		return nil, errBucketIsNotExist
	}

	for _, item := range items {
		if reflect.DeepEqual(key, item.key) {
			return item.value, nil
		}
	}

	return nil, errKeyIsNotFound
}
