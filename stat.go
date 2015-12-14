package magneticdb

import(
	"sync/atomic"
)

type Stat struct {
	numset  uint64
	numget  uint64
}

func NewStat()*Stat {
	stat := new(Stat)
	stat.numset = 0
	stat.numget = 0
	return stat
}

func (stat* Stat) IncSet() {
	atomic.AddUint64(&stat.numset, 1)
}

func (stat* Stat) IncGet() {
	atomic.AddUint64(&stat.numget, 1)
}