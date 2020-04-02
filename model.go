package dao

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Logoutputer func(s string)

type Execer interface {
	Exec(s string, args ...interface{}) (int64, error)
}

type Dao struct {
	database *sql.DB
	logger   Logoutputer
}

func NewDao(dsn string) (*Dao, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	return &Dao{database: db}, nil
}

func (dao *Dao) SetLogger(logger Logoutputer) {
	dao.logger = logger
}

func (dao *Dao) DB() *sql.DB {
	return dao.database
}

type Context struct {
	id  string
	dao *Dao
}

func (dao *Dao) CreateContext(id string) *Context {
	return &Context{id: id, dao: dao}
}

func (c *Context) logOutput(s string, args ...interface{}) {
	if c.dao.logger != nil {
		str := fmt.Sprintf("{ %s } %s %v", c.id, s, args)
		c.dao.logger(str)
	}
}

func (c *Context) Query(s string, args ...interface{}) *QueryContext {
	sqls, sqlv := placeholderExpansion(s, args...)
	c.logOutput(sqls, sqlv...)
	var ctx QueryContext
	rows, err := c.dao.database.Query(sqls, sqlv...)
	if err != nil {
		ctx.lastErr = err
	} else {
		ctx.rows = rows
	}
	return &ctx
}

func (c *Context) Exec(s string, args ...interface{}) (int64, error) {
	c.logOutput(s, args...)
	ret, err := c.dao.database.Exec(s, args...)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (c *Context) begin() (*Tx, error) {
	tx, err := c.dao.database.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{p: c, o: tx}, nil
}

func (c *Context) Tx(f func(*Tx) error) error {
	c.logOutput("begin")
	tx, err := c.begin()
	if err != nil {
		return err
	}
	if err = f(tx); err != nil {
		tx.rollback()
		c.logOutput("rollback")
		return err
	}
	c.logOutput("commit")
	return tx.commit()
}

/*
+---------------------------------------------+
+                     Tx                      +
+---------------------------------------------+
*/

type Tx struct {
	p *Context
	o *sql.Tx
}

func (tx *Tx) Exec(s string, args ...interface{}) (int64, error) {
	tx.p.logOutput(s, args...)
	ret, err := tx.o.Exec(s, args...)
	if err != nil {
		return 0, err
	}
	return ret.RowsAffected()
}

func (tx *Tx) Query(s string, args ...interface{}) *QueryContext {
	sqls, sqlv := placeholderExpansion(s, args...)
	tx.p.logOutput(sqls, sqlv...)
	var ctx QueryContext
	rows, err := tx.o.Query(sqls, sqlv...)
	if err != nil {
		ctx.lastErr = err
	} else {
		ctx.rows = rows
	}
	return &ctx
}

func (tx *Tx) Insert(table string, obj interface{}) (int64, error) {
	return insertOnUpdateBuilder(tx, table, nil, obj)
}

func (tx *Tx) InsertOnUpdate(table string, updateFields []string, obj interface{}) (int64, error) {
	return insertOnUpdateBuilder(tx, table, updateFields, obj)
}

func (tx *Tx) rollback() error {
	return tx.o.Rollback()
}

func (tx *Tx) commit() error {
	return tx.o.Commit()
}
