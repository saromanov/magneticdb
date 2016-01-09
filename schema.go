package magneticdb

import(
   "reflect"
   "fmt"
)

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

// ValidateSchema provides validation of schema and return true
// if schema is valid and false otherwise
func ValidateSchema(schema *Schema)bool {
	start := reflect.ValueOf(schema).MapKeys()
	fmt.Println(start)
	/*for _, item := range start {
		fmt.Println(reflect.TypeOf(item).Name())
	}*/
	return true
}
