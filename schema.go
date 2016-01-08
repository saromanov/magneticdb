package magneticdb


// Schema provides default definition of schema
type Schema struct {
	Tables []*Table
}

// Table must contain name and list of clumns
type Table struct {
	Name string
	Columns []*Column
}

// Column must contain name
type Column struct {
	Name  string
}
