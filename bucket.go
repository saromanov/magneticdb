package magneticdb

import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
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
	compress bool
}

type Bucket struct {
	items     map[string][]*Item
	keysize   uint
	valuesize uint
	compress bool

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

		b.compress = cfg.compress
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
	title = preprocessName(title)
	_, ok := b.items[title]
	if !ok {
		return errBucketIsNotExist
	}

	if b.compress {
		valueData, err := compress(value)
		if err != nil {
			return err
		}

		value = valueData
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
	item, err := b.read(title, key)
	if err != nil {
		return []byte{}, err
	}
	if b.compress {
		result, err := decompress(item.value)
		if err != nil {
			return nil, err
		}

		return result, nil
	}
	return item.value, nil
}

func (b *Bucket) GetStatForKey(title string, key[]byte) (uint64, error) {
	item, err := b.read(title, key)
	if err != nil {
		return 0, err
	}

	return item.readCount, nil
}

func (b *Bucket) Buckets()([]string, error) {
	items :=  make([]string, 0, len(b.items))
	for key, _ := range b.items {
		items = append(items, key)
	}

	sort.Strings(items)

	return items, nil
}

func (b *Bucket) read(title string, key []byte) (*Item, error) {
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	items, ok := b.items[title]
	if !ok {
		return nil, errBucketIsNotExist
	}

	for _, item := range items {
		if bytes.Equal(key, item.key) {
			atomic.AddUint64(&item.readCount, 1)
			return item, nil
		}
	}

	return nil, errKeyIsNotFound
}
