package magneticdb
import (
  "os"
  "sync"
  "errors"
  "fmt"
  "time"
  //"log"
)

var (
	errNotSupportWrite = errors.New("Write is not supported in read-only transaction")
	errNotSupportRead = errors.New("Read is not supported in write-only transaction")
	errEmptyKey = errors.New("Key must contain data")
	errEmptyValue = errors.New("Value must contain data")
)

// MagneticdbOpt provides options 
// for configuration MagneticDB
type MagneticdbOpt struct {
	Snapshot time.Duration
	SnapshotPath string
	Log     *LoggerConfig
}

// Magneticdb provides main struct
type Magneticdb struct {
	keysizelimit uint
	valuesizelimit uint
	readonly bool
	shanpshot time.Duration
	shanshotpath string
	path       string
	CommitFile *os.File
	index      *Index
	buckets    *Bucket
	stat       *Stat
	logger     *Logger

	commitlock *sync.RWMutex
	statlock *sync.RWMutex
	oplock *sync.RWMutex
}

// New provides setnew path to DB
func New(path string, open bool, opt *MagneticdbOpt) (*Magneticdb, error){
	if opt == nil {
		opt = defaultParams()
	}

	mdb := &Magneticdb {
		keysizelimit: 20,
		valuesizelimit: 1000,
		readonly: false,
		path:     path,
		commitlock: &sync.RWMutex{},
		statlock: &sync.RWMutex{},
		oplock: &sync.RWMutex{},
		index: NewIndex(),
		stat: NewStat(),
	}
	var err error
	var f *os.File
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

// Set provides insert key-value item
func (mdb *Magneticdb) Set(bucketname, key, value string) error{
	if mdb.readonly {
		mdb.logger.Info("Magneticdb in readomly mode")
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

// Commit provides commit changes to the disk
func (mdb *Magneticdb) Commit() error {
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
func (mdb *Magneticdb) Stat()map[string] string {
	return map[string] string {
		"numgets": fmt.Sprintf("%d", mdb.stat.numget),
		"numsets": fmt.Sprintf("%d", mdb.stat.numset),
	}
}

// String return string representation(Path) for magneticdb
func (mdb *Magneticdb) String() string {
	return fmt.Sprintf("Path: %s", mdb.path)
}

// Close provides closing current session od Magneticdb
func (mdb *Magneticdb) Close() {
	mdb.CommitFile.Close()
}

// create new file
func (mdb *Magneticdb) createPath(path string) (*os.File, error) {
	item, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0666)
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

// provide default options for Magneticdb
func defaultParams()*MagneticdbOpt {
	return &MagneticdbOpt {
		Snapshot: 10 * time.Second,
		SnapshotPath: "magneticdb.snapshow",
	}
}