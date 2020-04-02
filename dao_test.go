package dao

import (
	"fmt"
	"testing"
)

func TestQueryStrParse(t *testing.T) {
	s, v := placeholderExpansion("select * from t where id in (?) and age>?", []int{10, 20, 5}, 30)
	fmt.Println(s, v)
}
