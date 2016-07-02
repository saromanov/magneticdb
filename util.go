package magneticdb

import (
	"strings"
)

func preprocessName(name string) string {
	return strings.ToLower(name)
}