package magneticdb

import (
   "testing"
   "fmt"
   "os"
)

var (
	dbname = "this.db"
)

func removeDB(){
	os.Remove(dbname)
}

func TestNew(t *testing.T) {
	var err error
	_, err = New(dbname, false, nil)
	if err != nil {
		t.Errorf(fmt.Sprintf("%v", err))
	}
	defer removeDB()
	_, err = New(dbname, true, nil)
	if err != nil {
		t.Errorf(fmt.Sprintf("%v", err))
	}
}

func TestCreateBucket(t *testing.T) {
	var err error
	item, _ := New(dbname, false, nil)
	defer removeDB()
	err = item.CreateBucket("testbucket", nil)
	if err != nil {
		t.Errorf(fmt.Sprintf("%v", err))
	}
}