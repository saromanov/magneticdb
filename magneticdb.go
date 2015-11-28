package magneticdb
import (
  "os"
  "sync"
  "errors"
  "fmt"
  "time"
)

var (
	errNotSupportWrite = errors.New("Write is not supported in read-only transaction")
	errEmptyKey = errors.New("Key must contain data")
	errEmptyValue = errors.New("Value must contain data")
)

// MagneticdbOpt provides options 
// for configuration MagneticDB
type MagneticdbOpt struct {
	Snapshot time.Duration
	SnapshotPath string
}

type Magneticdb struct {
	keysizelimit uint
	valuesizelimit uint
	readonly bool
	shanpshot time.Duration
	shanshotpath string
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
		keysizelimit: 25,
		valuesizelimit: 1000,
		readonly: false,
		commitlock: &sync.RWMutex{},
		statlock: &sync.RWMutex{},
		oplock: &sync.RWMutex{},
	}
	var err error
	if open {
		err = mdb.openPath(path)
	} else {
		err = mdb.createPath(path)
	}

	if err != nil {
		return nil, err
	}

	return mdb, nil
}

// Set provides insert key-value item
func (mdb *Magneticdb) Set(key, value string) error{
	if mdb.readonly {
		return errNotSupportWrite
	}

	if mdb.keysizelimit < uint(len(key)) {
		return fmt.Errorf("Key size must be < %d", mdb.keysizelimit)
	}

	if mdb.valuesizelimit < uint(len(value)) {
		return fmt.Errorf("Value size must be < %d", mdb.valuesizelimit)
	}

	if key == "" {
		return errEmptyKey
	}

	if value == "" {
		return errEmptyValue
	}
	return nil
}

// Get provides getting value by key
func (mdb *Magneticdb) Get(key string) error {
	return nil
}

func (mdb *Magneticdb) SetReadonly(value bool) {
	mdb.readonly = value
}

func (mdb *Magneticdb) Close() {

}

// create new file
func (mdb *Magneticdb) createPath(path string) error {
	_, err := os.OpenFile(path, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (mdb *Magneticdb) openPath(path string) error {
	item, err := os.OpenFile(path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	_, errinfo := item.Stat()
	if errinfo == nil {
		return errinfo
	}
	return nil
}

// provide default options for Magneticdb
func defaultParams()*MagneticdbOpt {
	return &MagneticdbOpt {
		Snapshot: 10 * time.Second,
		SnapshotPath: "magneticdb.snapshow",
	}
}