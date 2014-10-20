package sandra

import (
	"testing"

	"github.com/gocql/gocql"
	. "gopkg.in/check.v1"
)

func TestCassandra(t *testing.T) { TestingT(t) }

type CassandraSuite struct {
	cassandra      Cassandra
	errorCassandra Cassandra
}

var _ = Suite(&CassandraSuite{})

func (s *CassandraSuite) SetUpSuite(c *C) {
	cassandra, err := NewCassandra(
		NewTestCassandraConfig(
			[]string{"localhost"}, "cassandra_test"))

	if err != nil {
		c.Fatal(err)
	}

	s.cassandra = cassandra
	s.errorCassandra = &TestErrorCassandra{}

	// create a table that will be used in tests
	if err = s.cassandra.ExecuteQuery("create table if not exists test (field int primary key)"); err != nil {
		c.Fatal(err)
	}
}

func (s *CassandraSuite) SetUpTest(c *C) {
	// clear test table before each test
	if err := s.cassandra.ExecuteQuery("truncate test"); err != nil {
		c.Fatal(err)
	}
}

func (s *CassandraSuite) TestNewCassandraConfig(c *C) {
	config, _ := NewCassandraConfig([]string{"1.1.1.1"}, "my_keyspace", "localquorum")
	c.Assert(config.Nodes, DeepEquals, []string{"1.1.1.1"})
	c.Assert(config.Keyspace, Equals, "my_keyspace")
	c.Assert(config.Consistency, Equals, gocql.LocalQuorum)
	c.Assert(config.TestMode, Equals, false)
}

func (s *CassandraSuite) TestNewTestCassandraConfig(c *C) {
	config := NewTestCassandraConfig([]string{"1.1.1.1"}, "my_keyspace")
	c.Assert(config.Nodes, DeepEquals, []string{"1.1.1.1"})
	c.Assert(config.Keyspace, Equals, "my_keyspace")
	c.Assert(config.Consistency, Equals, gocql.One)
	c.Assert(config.TestMode, Equals, true)
}

func (s *CassandraSuite) TestExecuteQuerySuccess(c *C) {
	err := s.cassandra.ExecuteQuery("insert into test (field) values (1)")
	c.Assert(err, IsNil)
}

func (s *CassandraSuite) TestExecuteQueryError(c *C) {
	err := s.cassandra.ExecuteQuery("drop table unknown")
	c.Assert(err, NotNil)
}

func (s *CassandraSuite) TestExecuteBatchSuccess(c *C) {
	queries := []string{
		"insert into test (field) values (?)",
		"insert into test (field) values (?)",
	}
	params := make([][]interface{}, 2)
	params[0] = []interface{}{11}
	params[1] = []interface{}{12}
	err := s.cassandra.ExecuteBatch(gocql.UnloggedBatch, queries, params)
	c.Assert(err, IsNil)
}

func (s *CassandraSuite) TestExecuteBatchError(c *C) {
	queries := []string{"", ""}
	err := s.cassandra.ExecuteBatch(gocql.UnloggedBatch, queries, [][]interface{}{})
	c.Assert(err, NotNil)
}

func (s *CassandraSuite) TestScanQuerySuccess(c *C) {
	s.cassandra.ExecuteQuery("insert into test (field) values (1)")
	var field int
	err := s.cassandra.ScanQuery("select * from test", []interface{}{}, &field)
	c.Assert(err, IsNil)
	c.Assert(field, Equals, 1)
}

func (s *CassandraSuite) TestScanQueryNotFoundError(c *C) {
	var field int
	err := s.cassandra.ScanQuery("select * from test where field = 999", []interface{}{}, &field)
	c.Assert(err, Equals, NotFound)
}

func (s *CassandraSuite) TestScanQueryError(c *C) {
	var field int
	err := s.cassandra.ScanQuery("select * from unknown", []interface{}{}, &field)
	c.Assert(err, NotNil)
}

func (s *CassandraSuite) TestScanCASQuerySuccess(c *C) {
	var field int
	applied, err := s.cassandra.ScanCASQuery("insert into test (field) values (3) if not exists", []interface{}{}, &field)
	c.Assert(err, IsNil)
	c.Assert(applied, Equals, true)
}

func (s *CassandraSuite) TestScanCASQueryError(c *C) {
	var field int
	applied, err := s.cassandra.ScanCASQuery("insert into unknown (field) values (3) if not exists", []interface{}{}, &field)
	c.Assert(err, NotNil)
	c.Assert(applied, Equals, false)
}

func (s *CassandraSuite) TestIterQuerySuccess(c *C) {
	s.cassandra.ExecuteQuery("insert into test (field) values (1)")
	s.cassandra.ExecuteQuery("insert into test (field) values (2)")

	var field int
	iter := s.cassandra.IterQuery("select * from test", []interface{}{}, &field)

	// first iteration
	idx, has_next, err := iter()
	c.Assert(idx, Equals, 0)
	c.Assert(has_next, Equals, true)
	c.Assert(err, IsNil)
	c.Assert(field, Equals, 1)

	// second iteration
	idx, has_next, err = iter()
	c.Assert(idx, Equals, 1)
	c.Assert(has_next, Equals, true)
	c.Assert(err, IsNil)
	c.Assert(field, Equals, 2)

	// time to stop
	idx, has_next, err = iter()
	c.Assert(has_next, Equals, false)
}

func (s *CassandraSuite) TestIterQueryError(c *C) {
	iter := s.cassandra.IterQuery("select * from unknown", []interface{}{})
	idx, has_next, err := iter()
	c.Assert(idx, Equals, 0)
	c.Assert(has_next, Equals, true)
	c.Assert(err, NotNil)
}
