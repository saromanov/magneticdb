package magneticdb

import (
   "errors"
)

type Item struct {
	bucketname string
	key []byte
	value []byte
	limit uint
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