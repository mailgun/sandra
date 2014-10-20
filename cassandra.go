package sandra

import (
	"errors"
	"fmt"

	"github.com/gocql/gocql"
	"github.com/mailgun/log"
)

type Cassandra interface {
	ExecuteQuery(string, ...interface{}) error
	ExecuteBatch(gocql.BatchType, []string, [][]interface{}) error
	ExecuteUnloggedBatch([]string, [][]interface{}) error
	ScanQuery(string, []interface{}, ...interface{}) error
	ScanCASQuery(string, []interface{}, ...interface{}) (bool, error)
	IterQuery(string, []interface{}, ...interface{}) func() (int, bool, error)
	Close() error
}

type cassandra struct {
	session *gocql.Session
	config  CassandraConfig
}

type CassandraConfig struct {
	Nodes       []string
	Keyspace    string
	Consistency gocql.Consistency

	// TestMode affects whether a keyspace creation will be attempted on Cassandra initialization.
	TestMode bool
}

var NotFound = errors.New("Not found")

func NewCassandraConfig(nodes []string, keyspace, consistencyName string) (*CassandraConfig, error) {
	consistency, err := translateConsistency(consistencyName)
	if err != nil {
		return nil, err
	}
	return &CassandraConfig{
		Nodes:       nodes,
		Keyspace:    keyspace,
		Consistency: consistency,
	}, nil
}

func NewTestCassandraConfig(nodes []string, keyspace string) *CassandraConfig {
	config, _ := NewCassandraConfig(nodes, keyspace, "one")
	config.TestMode = true
	return config
}

func NewCassandra(config *CassandraConfig) (Cassandra, error) {
	log.Infof("Connecting to Cassandra with config: %v", config)

	// initialize connection
	cluster := gocql.NewCluster(config.Nodes...)
	cluster.Consistency = config.Consistency
	cluster.ProtoVersion = 2
	cluster.CQLVersion = "3.0.0"

	if config.TestMode == true {
		cluster.Keyspace = ""

		session, err := cluster.CreateSession()
		if err != nil {
			return nil, err
		}

		// initialize schema
		query := session.Query(
			fmt.Sprintf(
				`create keyspace if not exists %v
                   with replication = {
                     'class': 'SimpleStrategy',
                     'replication_factor': 1}`,
				config.Keyspace))

		if err := query.Exec(); err != nil {
			log.Errorf("Error creating keyspace: %v", err)
		}

		session.Close()
	}

	cluster.Keyspace = config.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandra{session: session}, nil
}

func (c *cassandra) Close() error {
	c.session.Close()
	return nil
}

func (c *cassandra) ExecuteQuery(queryString string, queryParams ...interface{}) error {
	return c.session.Query(queryString, queryParams...).Exec()
}

func (c *cassandra) ExecuteBatch(batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	count := len(queries)

	// quick sanity check
	if count != len(params) {
		return errors.New("Amount of queries and params does not match")
	}

	batch := gocql.NewBatch(batchType)
	for idx := 0; idx < count; idx++ {
		batch.Query(queries[idx], params[idx]...)
	}

	return c.session.ExecuteBatch(batch)
}

func (c *cassandra) ExecuteUnloggedBatch(queries []string, params [][]interface{}) error {
	return c.ExecuteBatch(gocql.UnloggedBatch, queries, params)
}

func (c *cassandra) ScanQuery(queryString string, queryParams []interface{}, outParams ...interface{}) error {
	if err := c.session.Query(queryString, queryParams...).Scan(outParams...); err != nil {
		if err == gocql.ErrNotFound {
			return NotFound
		}
		return err
	}
	return nil
}

// Execute a lightweight transaction (an UPDATE or INSERT statement containing an IF clause)
func (c *cassandra) ScanCASQuery(queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return c.session.Query(queryString, queryParams...).ScanCAS(outParams...)
}

func (c *cassandra) IterQuery(queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	iter := c.session.Query(queryString, queryParams...).Iter()
	idx := -1
	return func() (int, bool, error) {
		idx++
		if iter.Scan(outParams...) {
			return idx, true, nil
		}
		if err := iter.Close(); err != nil {
			return idx, true, err
		}
		return idx, false, nil
	}
}

// Return appropriate gocql.Consistency based on the provided consistency level name.
func translateConsistency(consistencyName string) (gocql.Consistency, error) {
	for index, name := range gocql.ConsistencyNames {
		if name == consistencyName {
			return gocql.Consistency(index), nil
		}
	}
	return gocql.One, fmt.Errorf("unknown consistency: %v", consistencyName)
}
