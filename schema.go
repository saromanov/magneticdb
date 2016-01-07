package magneticdb


type Schema struct {
	Tables []*Table
}

type Table struct {
	Name string
	Columns []*Column
}

type Column struct {
	Name  string
}
