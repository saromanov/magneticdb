package magneticdb

import (
	"errors"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
  "bytes"
	//"log"
)

var (
	errNotSupportWrite = errors.New("Write is not supported in read-only transaction")
	errNotSupportRead  = errors.New("Read is not supported in write-only transaction")
	errEmptyKey        = errors.New("Key must contain data")
	errEmptyValue      = errors.New("Value must contain data")
)

var (
  BEGIN = []byte("0k76")
  END = []byte("z7ok")
)

// MagneticdbOpt provides options
// for configuration MagneticDB
type MagneticdbOpt struct {
	Snapshot     time.Duration
	SnapshotPath string
	Log          *LoggerConfig
	Compress     bool
}

// Magneticdb provides main struct
type Magneticdb struct {
	keysizelimit   uint
	valuesizelimit uint
	readonly       bool
	shanpshot      time.Duration
	shanshotpath   string
	f              *os.File
	CommitFile     *os.File
	index          *Index
	buckets        *Bucket
	schemas        map[string]*Schema
	stat           *Stat
	logger         *Logger
	compress       bool

	commitlock *sync.RWMutex
	statlock   *sync.RWMutex
	oplock     *sync.RWMutex
}

// New provides setnew path to DB
func New(f *os.File, open bool, opt *MagneticdbOpt) (*Magneticdb, error) {
	if opt == nil {
		opt = defaultParams()
	}

	path := "default"
	mdb := &Magneticdb{
		keysizelimit:   20,
		valuesizelimit: 1000,
		readonly:       false,
		f:              f,
		//path:           path,
		schemas:        map[string]*Schema{},
		commitlock:     &sync.RWMutex{},
		statlock:       &sync.RWMutex{},
		oplock:         &sync.RWMutex{},
		index:          NewIndex(),
		stat:           NewStat(),
		compress:       opt.Compress,
	}
	var err error
	//var f *os.File
	if open {
		f, err = mdb.openPath(path)
	} else {
		f, err = mdb.createPath(path)
	}

	if err != nil {
		return nil, err
	}

	mdb.CommitFile = f
	mdb.logger = NewLogger(opt.Log)

	mdb.buckets = NewBucket()

	return mdb, nil
}

//CreateBucket provides creature of the new bucket
func (mdb *Magneticdb) CreateBucket(title string, cfg *BucketConfig) error {
	mdb.logger.Info(fmt.Sprintf("Create bucket %s", title))
	return mdb.buckets.CreateBucket(title, cfg)
}

// CreateIndex porvides new index to MagneticDB
func (mdb *Magneticdb) CreateIndex(title string) {
	mdb.index.CreateIndex(title)
}

// CreateSchema provides creational of the new schema
func (mdb *Magneticdb) CreateSchema(name string, schema *Schema) error {
	_, ok := mdb.schemas[name]
	if ok {
		return fmt.Errorf("Schema with the name %s already exist", name)
	}
	ValidateSchema(schema)
	mdb.schemas[name] = schema
	return nil
}

// Set provides insert key-value item with bucket name
func (mdb *Magneticdb) Set(bucketname, key, value string) error {
	if mdb.readonly {
		mdb.logger.Info("Magneticdb in readonly mode")
		return errNotSupportWrite
	}

	if mdb.keysizelimit < uint(len(key)) {
		mdb.logger.Error(fmt.Sprintf("Key size must be < %d", mdb.keysizelimit))
		return fmt.Errorf("Key size must be < %d", mdb.keysizelimit)
	}

	if mdb.valuesizelimit < uint(len(value)) {
		return fmt.Errorf("Value size must be < %d", mdb.valuesizelimit)
	}

	if key == "" {
		mdb.logger.Error("Key is empty")
		return errEmptyKey
	}

	if value == "" {
		mdb.logger.Error("Value is empty")
		return errEmptyValue
	}
	mdb.oplock.Lock()
	defer mdb.oplock.Unlock()

	keybyte := []byte(key)
	valuebyte := []byte(value)
	mdb.logger.Info("Set value to index")
	mdb.index.Put(keybyte)
	mdb.logger.Info(fmt.Sprintf("Set to the bucket %s", bucketname))
	mdb.buckets.SetToBucket(bucketname, keybyte, valuebyte)
	mdb.stat.IncSet()
	return nil
}

// Get provides getting value by key
func (mdb *Magneticdb) Get(bucketname, key string) (string, error) {
	if !mdb.readonly {
		return "", errNotSupportRead
	}
	keybyte := []byte(key)
	mdb.logger.Info(fmt.Sprintf("Getting from the bucket bucket %s by key %s", bucketname, key))
	valuebyte, err := mdb.buckets.GetFromBucket(bucketname, keybyte)
	if err != nil {
		return "", err
	}
	mdb.stat.IncGet()
	return string(valuebyte), nil
}

func (mdb *Magneticdb) GetStatForKey(bucketname, key string) error {
	if !mdb.readonly {
		return errNotSupportRead
	}

	return nil
}

// Buckets returns list of the buckets
func (mdb *Magneticdb) Buckets() ([]string, error) {
	return mdb.buckets.Buckets()
}

// Commit provides commit changes to the disk
func (mdb *Magneticdb) Commit() error {
	if mdb.readonly {
		return errors.New("read-only mode")
	}
	mdb.commitlock.Lock()
	defer mdb.commitlock.Unlock()
	lastchange := time.Now().String()
	mdb.CommitFile.Write([]byte(lastchange))
	return nil
}

// SetReadonly provides setting only read transaction
func (mdb *Magneticdb) SetReadonly(value bool) {
	mdb.readonly = value
}

// Info provides information by key-value pair
func (mdb *Magneticdb) InfoItem(key string) {

}

// Stat return information about statictics
func (mdb *Magneticdb) Stat() map[string]string {
	return map[string]string{
		"numgets": fmt.Sprintf("%d", mdb.stat.numget),
		"numsets": fmt.Sprintf("%d", mdb.stat.numset),
	}
}

// String return string representation(Path) for magneticdb
func (mdb *Magneticdb) String() string {
	return fmt.Sprintf("Path: %s")
}

// Close provides closing current session od Magneticdb
func (mdb *Magneticdb) Close() {
	mdb.CommitFile.Close()
}

// create new file
func (mdb *Magneticdb) createPath(path string) (*os.File, error) {
	item, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	return item, nil
}

// openPath provides open db data
func (mdb *Magneticdb) openPath(path string) (*os.File, error) {
	item, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	_, errinfo := item.Stat()
	if errinfo == nil {
		return nil, errinfo
	}
	return item, nil
}

// Flush: write data to teh disk
func (mdb *Magneticdb) Flush() error {
   result, err := json.Marshal(mdb.Buckets)
   if err != nil {
     return err
   }

  length := len(BEGIN)+2 + len(result)
	b := bytes.NewBuffer(make([]byte, length)[:0])
	b.Write(BEGIN)
	b.Write(result)
	b.Write(END)

	return nil

}

// provide default options for Magneticdb
func defaultParams() *MagneticdbOpt {
	return &MagneticdbOpt{
		Snapshot:     10 * time.Second,
		SnapshotPath: "magneticdb.snapshow",
	}
}
