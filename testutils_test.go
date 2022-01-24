package sandra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type TestErrorCassandra struct{}

func (c *TestErrorCassandra) Query(consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query {
	return nil
}

func (c *TestErrorCassandra) Config() CassandraConfig {
	return CassandraConfig{}
}

func (c *TestErrorCassandra) Session() *gocql.Session {
	return nil
}

func (c *TestErrorCassandra) ExecuteQueryCtx(_ context.Context, queryString string, queryParams ...interface{}) error {
	return fmt.Errorf("error during ExecuteQueryCtx")
}

func (c *TestErrorCassandra) ExecuteQuery(queryString string, queryParams ...interface{}) error {
	return fmt.Errorf("Error during ExecuteQuery")
}

func (c *TestErrorCassandra) ExecuteBatch(batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteBatch")
}

func (c *TestErrorCassandra) ExecuteUnloggedBatch(queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteUnloggedBatch")
}

func (c *TestErrorCassandra) ScanQuery(queryString string, queryParams []interface{}, outParams ...interface{}) error {
	return fmt.Errorf("Error during ScanQuery")
}

func (c *TestErrorCassandra) ScanCASQuery(queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return false, fmt.Errorf("Error during ScanCASQuery")
}

func (c *TestErrorCassandra) IterQuery(queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	return func() (int, bool, error) {
		return 0, true, fmt.Errorf("Error during IterQuery")
	}
}

func (c *TestErrorCassandra) Close() error {
	return fmt.Errorf("Error during Close")
}

func TableExists(db Cassandra, table string) (bool, error) {
	var tableName string
	// Only tested with Cassandra 3.11.x
	iter := db.IterQuery("SELECT table_name FROM system_schema.tables"+
		" WHERE keyspace_name = ? AND table_name = ?",
		[]interface{}{db.Config().Keyspace, table}, &tableName)
	_, _, err := iter()

	if err != nil {
		return false, err
	}

	// If isn't empty, table exists
	if tableName != "" {
		return true, nil
	}
	return false, nil
}

func WaitForTables(db Cassandra, timeout time.Duration, tables ...string) error {
	quit := false
	mutex := sync.Mutex{}
	time.AfterFunc(timeout, func() {
		mutex.Lock()
		quit = true
		mutex.Unlock()
	})

	for _, table := range tables {
	tryAgain:
		mutex.Lock()
		exists, err := TableExists(db, table)
		if err != nil {
			mutex.Unlock()
			return err
		}

		if exists {
			mutex.Unlock()
			break
		}

		if quit {
			mutex.Unlock()
			return errors.Errorf("timeout waiting for table '%s'", table)
		}
		mutex.Unlock()
		time.Sleep(time.Millisecond * 500)
		goto tryAgain
	}
	return nil
}
