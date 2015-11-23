package magneticdb
import (
  "os/ioutil"
  "os"
  "log"
  "sync"
  "errors"
)

var (
	errNotSupportWrite = errors.New("Write is not supported in read-only transaction")
)

type Magneticdb struct {
	keysizelimit uint
	valuesizelimit uint
	readonly bool
	commitlock *sync.RWMutex
	statlock *sync.RWMutex
	oplock *sync.RWMutex
}

// New provides setnew path to DB
func New(path string, open bool) (*Magneticdb, error){

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
	if mgb.readonly {
		return errNotSupportWrite
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
	item, err := ioutil.Open(path, os.O_RDWR | os.O_CREATE, 0666)
	if err != nil {
		return err
	}

	return nil
}

func (mdb *Magneticdb) openPath(path string) err {
	item, err := ioutil.Open(path, os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	info, errinfo := item.Stat()
	if errinfo == nil {
		return err
	}
}