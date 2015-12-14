package magneticdb


type Schema struct {
	Tables map[string]*Table
}

type Table struct {
	Name string
}