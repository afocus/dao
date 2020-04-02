package dao

import (
	"reflect"
	"strings"
)

func lowUpperString(s string) string {
	b := make([]byte, 0)
	last := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
			if i != 0 && last != i-1 {
				b = append(b, '_')
			}
			last = i
		}
		b = append(b, c)
	}
	return string(b)
}

func parseSQLArgsInSlice(count int) string {
	s := strings.Split(strings.Repeat("?", count), "")
	return strings.Join(s, ",")
}

// placeholderExpansion 占位符展开
// where name = ? and id in (?)  ["afocus", []{1,3,4,5}] 展开为 where name = ? and id in (?,?,?,?)  ["afocus", 1,3,4,5]
func placeholderExpansion(s string, args ...interface{}) (string, []interface{}) {
	var (
		values []interface{}
		ns     []byte
		n      int
	)
	for i := 0; i < len(s); i++ {
		if s[i] == '?' {
			if s[i-1] == '(' {
				tv := reflect.ValueOf(args[n])
				x := tv.Type()
				if x.Kind() == reflect.Slice {
					length := tv.Len()
					ns = append(ns, parseSQLArgsInSlice(length)...)
					for j := 0; j < length; j++ {
						values = append(values, tv.Index(j).Interface())
					}
				} else {
					ns = append(ns, '?')
					values = append(values, tv.Interface())
				}
				n++
				continue
			}
			values = append(values, args[n])
			n++
		}
		ns = append(ns, s[i])
	}
	return string(ns), values
}
