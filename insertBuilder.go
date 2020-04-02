package dao

import (
	"fmt"
	"reflect"
	"strings"
)

func insertOneBuilder(e Execer, table string, updatefields []string, v reflect.Value) (int64, error) {
	var fields []string
	var args []interface{}
	switch v.Type().Kind() {
	case reflect.Struct:
		fields, args = parseStruct(v)
	case reflect.Map:
		fields, args = parseMap(v)
	default:
		return 0, errorRowData
	}
	quto := make([]string, len(fields))
	for i, v := range fields {
		quto[i] = "?"
		fields[i] = fmt.Sprintf("`%s`", v)
	}
	const sqltpl = "insert into %s(%s) values (%s)"
	s := fmt.Sprintf(sqltpl, table, strings.Join(fields, ","), strings.Join(quto, ","))
	if len(updatefields) > 0 {
		s += " on duplicate key update "
		for _, v := range updatefields {
			s += fmt.Sprintf("`%s`=values(`%s`),", v, v)
		}
		s = s[:len(s)-1]
	}
	return e.Exec(s, args...)
}

func insertOnUpdateBuilder(e Execer, table string, updateFields []string, obj interface{}) (int64, error) {
	v := reflect.Indirect(reflect.ValueOf(obj))
todo:
	switch v.Kind() {
	case reflect.Ptr:
		v = v.Elem()
		goto todo
	case reflect.Map, reflect.Struct:
		return insertOneBuilder(e, table, updateFields, v)
	case reflect.Slice:
		var retcount int64
		for i := 0; i < v.Len(); i++ {
			ret, err := insertOneBuilder(e, table, updateFields, v.Index(i))
			if err != nil {
				return retcount, err
			}
			retcount += ret
		}
		return retcount, nil
	default:
		return 0, errorRowData
	}
}
