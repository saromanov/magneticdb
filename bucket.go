package magneticdb

import (
	"bytes"
	"errors"
	"sync"
)

var (
	errBucketIsNotExist = errors.New("Bucket is not exist")
	errKeyIsNotFound    = errors.New("Key is not found")
	errBucketExist      = errors.New("Bucket already exist")
)

// BucketConfig provides configuration for each bucket.
// Optional parameter for CreateBucket
type BucketConfig struct {
	keysize   uint
	valuesize uint
}

type Bucket struct {
	items     map[string][]*Item
	keysize   uint
	valuesize uint

	mutex *sync.RWMutex
}

// New provides creational of the new bucket
func NewBucket() *Bucket {
	b := new(Bucket)
	b.items = map[string][]*Item{}
	return b
}

// CreateBucket provides creational of the new bucket
func (b *Bucket) CreateBucket(title string, cfg *BucketConfig) error {
	if cfg != nil {
		if cfg.keysize != 0 {
			b.keysize = cfg.keysize
		}

		if cfg.valuesize != 0 {
			b.valuesize = cfg.valuesize
		}
	}

	title = preprocessName(title)
	_, ok := b.items[title]
	if ok {
		return errBucketExist
	}
	b.items[title] = []*Item{}
	b.mutex = &sync.RWMutex{}
	return nil
}

// SetToBucket provides setting ley-value to new bucket
func (b *Bucket) SetToBucket(title string, key, value []byte) error {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	_, ok := b.items[title]
	if !ok {
		return errBucketIsNotExist
	}

	if b.keysize != 0 && b.keysize > uint(len(key)) {

	}

	newitem, err := set(title, key, value)
	if err != nil {
		return err
	}
	b.items[title] = append(b.items[title], newitem)
	return nil
}

// SetTagsToItemFromBucket provides setting tags to item from bucket
func (b *Bucket) SetTagsToItemFromBucket(title string, key []byte, tags []string) error {
	items, ok := b.items[title]
	if !ok {
		return errBucketIsNotExist
	}

	for _, item := range items {
		if bytes.Equal(key, item.key) {
			item.tags = tags
			goto EXIT
		}
	}
EXIT:
	return nil
}

//GetFromBucket provides getting value fron item from bucket
func (b *Bucket) GetFromBucket(title string, key []byte) ([]byte, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	items, ok := b.items[title]
	if !ok {
		return nil, errBucketIsNotExist
	}

	for _, item := range items {
		if bytes.Equal(key, item.key) {
			return item.value, nil
		}
	}

	return nil, errKeyIsNotFound
}

func (b *Bucket) Buckets()([]string, error) {
	items := make([]string, len(b.items))
	i := 0
	for key, _ := range b.items {
		items[i] = key
		i++
	}

	return items, nil
}
