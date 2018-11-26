package sandra

import (
	"fmt"
	"time"

	"sync"

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
		var tableName string
		// Only works for Cassandra 1.1 - 2.0 versions
		iter := db.IterQuery("SELECT columnfamily_name FROM system.schema_columnfamilies"+
			" WHERE keyspace_name = ? AND columnfamily_name = ?",
			[]interface{}{db.Config().Keyspace, table}, &tableName)
		_, _, err := iter()
		if err == nil {
			return err
		}
		// If isn't empty, table exists
		if tableName != "" {
			continue
		}
		mutex.Lock()
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
