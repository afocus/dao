package dao

import (
	"bytes"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
)

type Session struct {
	uniq   string
	dao    *Dao
	table  string
	fields []string
	cond   *sessionCond
	parts  []expPart
	tx     *sql.Tx

	querySrcData *sqlData
}

type sqlData struct {
	Query string
	Args  []interface{}
}

type expPart struct {
	name   string
	values []string
}

func ExpPart(name string, v ...string) expPart {
	return expPart{name: name, values: v}
}

func (e *expPart) String() string {
	return fmt.Sprintf(" %s %s", e.name, strings.Join(e.values, ", "))
}

var sessions = &sync.Pool{
	New: func() interface{} {
		return &Session{}
	},
}

func (s *Session) logOutput(query string, args interface{}) {
	if s.dao.logger != nil {
		str := fmt.Sprintf("[%s] %s %v", s.uniq, query, args)
		s.dao.logger(str)
	}
}

func (s *Session) Close() {
	s.reset()
	sessions.Put(s)
}

func (s *Session) Table(t string) *Session {
	s.table = t
	return s
}

func (s *Session) Where(query string, args ...interface{}) *Session {
	s.cond = newSessionCond(query, args...)
	return s
}

func (s *Session) Cols(field ...string) *Session {
	s.fields = field
	return s
}

func (s *Session) reset() {
	s.fields = nil
	s.parts = nil
	s.cond = nil
	s.tx = nil
	s.querySrcData = nil
}

func (s *Session) Exec(query string, args ...interface{}) (int64, error) {

	var (
		ret sql.Result
		err error
	)
	s.logOutput(query, args)
	if s.tx != nil {
		ret, err = s.tx.Exec(query, args...)
	} else {
		ret, err = s.dao.DB().Exec(query, args...)
	}
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (s *Session) Insert(obj interface{}) (int64, error) {
	query, args, err := s.insertBuilder(s.table, s.fields, obj)
	if err != nil {
		return 0, err
	}
	return s.Exec(query, args...)
}

func (s *Session) insertBuilder(table string, updatefields []string, obj interface{}) (string, []interface{}, error) {
	v := reflect.Indirect(reflect.ValueOf(obj))
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	var (
		fields []string
		args   []interface{}
		skipPk = -1
		sqltpl = "insert into %s(%s) values (%s)"
	)

	switch v.Type().Kind() {
	case reflect.Struct:
		fields, args = parseStruct(v)
	case reflect.Map:
		fields, args = parseMap(v)
	default:
		return "", nil, errorRowData
	}

	quto := make([]string, len(fields))
	for i, v := range fields {
		quto[i] = "?"
		fields[i] = fmt.Sprintf("`%s`", v)
		if v == "id" {
			if args[i] == nil {
				skipPk = i
				continue
			}
			if reflect.ValueOf(args[i]).IsZero() {
				skipPk = i
				continue
			}
		}
	}
	if skipPk >= 0 {
		quto = append(quto[:skipPk], quto[skipPk+1:]...)
		fields = append(fields[:skipPk], fields[skipPk+1:]...)
		args = append(args[:skipPk], args[skipPk+1:]...)
	}

	query := fmt.Sprintf(sqltpl, table, strings.Join(fields, ","), strings.Join(quto, ","))
	if len(updatefields) > 0 {
		query += " on duplicate key update "
		for _, v := range updatefields {
			query += fmt.Sprintf("`%s` = values(`%s`),", v, v)
		}
		query = query[:len(query)-1]
	}
	return query, args, nil
}

func (s *Session) Delete() (int64, error) {
	if s.table == "" {
		return 0, errors.New("tablename faild")
	}
	str := bytes.NewBuffer(nil)
	str.WriteString(fmt.Sprintf("delete from %s", s.table))
	condstr, condargs := s.cond.Build()
	str.WriteString(condstr)

	for _, v := range s.parts {
		str.WriteString(v.String())
	}
	return s.Exec(str.String(), condargs...)
}

func (s *Session) Update(obj interface{}) (int64, error) {
	v := reflect.Indirect(reflect.ValueOf(obj))
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	var (
		keys   []string
		values []interface{}
		str    = bytes.NewBuffer(nil)
	)

	switch v.Kind() {
	case reflect.Map:
		keys, values = parseMap(v)
	case reflect.Struct:
		keys, values = parseStruct(v)
	}

	str.WriteString(fmt.Sprintf("update %s set", s.table))

	if len(s.fields) > 0 {
		val := make([]interface{}, 0)
		for _, v := range s.fields {
			for i, a := range keys {
				if a == v {
					str.WriteString(fmt.Sprintf(" %s = ?,", v))
					val = append(val, values[i])
					break
				}
			}
		}
		values = val
	} else {
		for _, a := range keys {
			str.WriteString(fmt.Sprintf(" %s = ?,", a))
		}
	}
	stro := str.String()
	stro = stro[:len(stro)-1]

	condstr, condargs := s.cond.Build()
	values = append(values, condargs...)
	return s.Exec(stro+condstr, values...)
}

func (s *Session) Tx(fn func(*Session) error) error {
	tx, err := s.dao.DB().Begin()
	if err != nil {
		return err
	}
	s.tx = tx
	defer func() {
		s.tx = nil
	}()
	if err = fn(s); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
