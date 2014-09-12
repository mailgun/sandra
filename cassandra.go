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
	Nodes             []string
	Keyspace          string
	ReplicationFactor int
	ConsistencyLevel  gocql.Consistency
}

var NotFound = errors.New("Not found")

func NewCassandraConfig(nodes []string, keyspace string, productionMode bool) *CassandraConfig {
	config := &CassandraConfig{
		Nodes:    nodes,
		Keyspace: keyspace,
	}

	if productionMode == true {
		config.ReplicationFactor = 3
		config.ConsistencyLevel = gocql.Two
	} else {
		config.ReplicationFactor = 1
		config.ConsistencyLevel = gocql.One
	}

	return config
}

func NewCassandra(config *CassandraConfig) (Cassandra, error) {
	log.Infof("Connecting to Cassandra with config %v", config)

	// initialize connection
	cluster := gocql.NewCluster(config.Nodes...)
	cluster.Keyspace = ""
	cluster.Consistency = config.ConsistencyLevel
	cluster.ProtoVersion = 2
	cluster.CQLVersion = "3.0.0"

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
                 'replication_factor': %v}`,
			config.Keyspace,
			config.ReplicationFactor))
	if err := query.Exec(); err != nil {
		log.Errorf("Error creating keyspace: %v", err)
	}

	session.Close()

	cluster.Keyspace = config.Keyspace

	session, err = cluster.CreateSession()
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
