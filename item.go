package magneticdb

import (
   "errors"
)

type Item struct {
	bucketname string
	key []byte
	value []byte
	limit uint
	tags []string
	readCount uint64
	writeCount uint64
}

func (item*Item) Copy()*Item {
	return &Item {
		bucketname: item.bucketname,
		key: item.key,
		value: item.value,
		limit: item.limit,
		tags: item.tags,
		readCount: item.readCount,
		writeCount: item.writeCount,
	}
}

var (
	errEmptyBucketName = errors.New("Length of the bucket name can't be equals to zero")
)

func set(bucketname string, key, value []byte)(*Item, error) {
	if len(bucketname) == 0 {
		return nil, errEmptyBucketName
	}

	return &Item {
		bucketname: bucketname,
		key: key,
		value: value,
	}, nil
}

func (item *Item) setTags(tags[]string){
	item.tags = tags
}