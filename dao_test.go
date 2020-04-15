package dao

import (
	"fmt"
	"testing"
)

func initDao() *Dao {
	c, _ := Create("root:123456@(127.0.0.1:3306)/test?charset=utf8")
	c.SetLogger(func(s string) { fmt.Println(s) })
	return c
}

func TestInsert(t *testing.T) {
	type TA struct {
		ID   int64
		Name string
	}

	s := initDao().NewSession()
	ret, err := s.Table("t_a").Insert(TA{Name: "aaaabb"})
	fmt.Println(ret, err)

	s.Close()

	s1 := initDao().NewSession()

	var data TA
	s1.Table("t_a").Select("name").Where("name = ?", "aaaabb").Get(&data)
}
