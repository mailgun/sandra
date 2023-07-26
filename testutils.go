package sandra

import (
	"context"
	"fmt"

	"github.com/gocql/gocql"
)

type TestErrorCassandra struct{}

func (c *TestErrorCassandra) QueryCtx(_ context.Context, consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query {
	return nil
}

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

func (c *TestErrorCassandra) ExecuteBatchCtx(_ context.Context, batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteBatchCtx")
}

func (c *TestErrorCassandra) ExecuteBatch(batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteBatch")
}

func (c *TestErrorCassandra) ExecuteUnloggedBatchCtx(_ context.Context, queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteUnloggedBatchCtx")
}

func (c *TestErrorCassandra) ExecuteUnloggedBatch(queries []string, params [][]interface{}) error {
	return fmt.Errorf("Error during ExecuteUnloggedBatch")
}

func (c *TestErrorCassandra) ScanQueryCtx(_ context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) error {
	return fmt.Errorf("Error during ScanQueryCtx")
}

func (c *TestErrorCassandra) ScanQuery(queryString string, queryParams []interface{}, outParams ...interface{}) error {
	return fmt.Errorf("Error during ScanQuery")
}

func (c *TestErrorCassandra) ScanCASQueryCtx(_ context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return false, fmt.Errorf("Error during ScanCASQueryCtx")
}

func (c *TestErrorCassandra) ScanCASQuery(queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return false, fmt.Errorf("Error during ScanCASQuery")
}

func (c *TestErrorCassandra) IterQueryCtx(_ context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	return func() (int, bool, error) {
		return 0, true, fmt.Errorf("Error during IterQueryCtx")
	}
}

func (c *TestErrorCassandra) IterQuery(queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	return func() (int, bool, error) {
		return 0, true, fmt.Errorf("Error during IterQuery")
	}
}

func (c *TestErrorCassandra) Close() error {
	return fmt.Errorf("Error during Close")
}
