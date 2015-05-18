package sandra

import (
	"errors"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/mailgun/log"
)

type Cassandra interface {
	GetQuery(gocql.Consistency, string, ...interface{}) *gocql.Query
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
	rcl     gocql.Consistency
	wcl     gocql.Consistency
}

// CassandraConfig is a json and yaml friendly configuration struct
type CassandraConfig struct {
	// Required Parameters
	Nodes            []string // addresses for the initial connections
	Keyspace         string   // initial keyspace
	ReadConsistency  string   // consistency for read operations
	WriteConsistency string   // consistency for write operations
	Timeout          string   `config:"optional"` // connection timeout (default: 600ms)
	KeepAlive        string   `config:"optional"` // The keepalive period to use default: 0
	NumConns         int      `config:"optional"` // number of connections per host (default: 2)
	NumStreams       int      `config:"optional"` // number of streams per connection (default: 128)
	Port             int      `config:"optional"` // port to connect to, default: 9042

	// TestMode affects whether a keyspace creation will be attempted on Cassandra initialization.
	TestMode bool `config:"optional"`
}

func (c CassandraConfig) String() string {
	return fmt.Sprintf("CassandraConfig(Nodes=%v, Keyspace=%v, ReadConsistency=%v, WriteConsistency=%v, TestMode=%v)",
		c.Nodes, c.Keyspace, c.ReadConsistency, c.WriteConsistency, c.TestMode)
}

var NotFound = errors.New("Not found")

func NewCassandra(config CassandraConfig) (Cassandra, error) {
	log.Infof("Connecting to Cassandra with config: %v", config)

	cluster, err := setDefaults(config)
	if err != nil {
		return nil, err
	}

	// in test mode, create a keyspace if necessary
	if config.TestMode == true {
		session, err := cluster.CreateSession()
		if err != nil {
			return nil, err
		}

		query := session.Query(
			fmt.Sprintf(
				`create keyspace if not exists %v
                   with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
				config.Keyspace))

		if err := query.Exec(); err != nil {
			log.Errorf("Error creating keyspace: %v", err)
		}

		session.Close()
	}

	// switch the keyspace
	cluster.Keyspace = config.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	rcl, err := translateConsistency(config.ReadConsistency)
	if err != nil {
		return nil, err
	}

	wcl, err := translateConsistency(config.WriteConsistency)
	if err != nil {
		return nil, err
	}

	return &cassandra{session, config, rcl, wcl}, nil
}

func (c *cassandra) Close() error {
	c.session.Close()
	return nil
}

// GetQuery provides an access to the gocql.Query if a user of this library needs to tune some parameters for
// a specific query without modifying the parameters the library was configured with, for example to use
// a consistency level that differs from the configured read/write consistency levels.
func (c *cassandra) GetQuery(consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query {
	return c.session.Query(queryString, queryParams...).Consistency(consistency)
}

func (c *cassandra) ExecuteQuery(queryString string, queryParams ...interface{}) error {
	return c.GetQuery(c.wcl, queryString, queryParams...).Exec()
}

func (c *cassandra) ExecuteBatch(batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	count := len(queries)

	// quick sanity check
	if count != len(params) {
		return errors.New("Amount of queries and params does not match")
	}

	batch := gocql.NewBatch(batchType)
	batch.Cons = c.wcl
	for idx := 0; idx < count; idx++ {
		batch.Query(queries[idx], params[idx]...)
	}

	return c.session.ExecuteBatch(batch)
}

func (c *cassandra) ExecuteUnloggedBatch(queries []string, params [][]interface{}) error {
	return c.ExecuteBatch(gocql.UnloggedBatch, queries, params)
}

func (c *cassandra) ScanQuery(queryString string, queryParams []interface{}, outParams ...interface{}) error {
	if err := c.GetQuery(c.rcl, queryString, queryParams...).Scan(outParams...); err != nil {
		if err == gocql.ErrNotFound {
			return NotFound
		}
		return err
	}
	return nil
}

// Execute a lightweight transaction (an UPDATE or INSERT statement containing an IF clause)
func (c *cassandra) ScanCASQuery(queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return c.GetQuery(c.rcl, queryString, queryParams...).ScanCAS(outParams...)
}

func (c *cassandra) IterQuery(queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	iter := c.GetQuery(c.rcl, queryString, queryParams...).Iter()
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

func translateDuration(k string, df time.Duration) (time.Duration, error) {
	if k == "" {
		return df, nil
	}
	return time.ParseDuration(k)
}

func setDefaults(cfg CassandraConfig) (*gocql.ClusterConfig, error) {
	keepAlive, err := translateDuration(cfg.KeepAlive, 0)
	if err != nil {
		return nil, err
	}

	timeout, err := translateDuration(cfg.Timeout, 600*time.Millisecond)
	if err != nil {
		return nil, err
	}

	if cfg.Port == 0 {
		cfg.Port = 9042
	}

	if cfg.NumConns == 0 {
		cfg.NumConns = 2
	}

	if cfg.NumStreams == 0 {
		cfg.NumStreams = 128
	}

	cluster := gocql.NewCluster(cfg.Nodes...)
	cluster.ProtoVersion = 2
	cluster.CQLVersion = "3.0.0"
	cluster.Timeout = timeout
	cluster.NumConns = cfg.NumConns
	cluster.NumStreams = cfg.NumStreams
	cluster.SocketKeepalive = keepAlive
	cluster.Port = cfg.Port

	return cluster, nil
}
