package sandra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
)

type Cassandra interface {
	QueryCtx(ctx context.Context, consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query
	Query(gocql.Consistency, string, ...interface{}) *gocql.Query
	ExecuteQueryCtx(ctx context.Context, queryString string, queryParams ...interface{}) error
	ExecuteQuery(string, ...interface{}) error
	ExecuteBatchCtx(ctx context.Context, batchType gocql.BatchType, queries []string, params [][]interface{}) error
	ExecuteBatch(gocql.BatchType, []string, [][]interface{}) error
	ExecuteUnloggedBatchCtx(ctx context.Context, queries []string, params [][]interface{}) error
	ExecuteUnloggedBatch([]string, [][]interface{}) error
	ScanQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) error
	ScanQuery(string, []interface{}, ...interface{}) error
	ScanCASQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) (applied bool, err error)
	ScanCASQuery(string, []interface{}, ...interface{}) (bool, error)
	IterQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) func() (idx int, hasNext bool, err error)
	IterQuery(string, []interface{}, ...interface{}) func() (int, bool, error)
	Close() error
	Config() CassandraConfig
	Session() *gocql.Session
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
	Nodes                    []string         `json:"nodes"`                        // addresses for the initial connections
	DataCenter               string           `json:"datacenter" config:"optional"` // data center name
	Keyspace                 string           `json:"keyspace"`                     // initial keyspace
	ReadConsistency          string           `json:"readconsistency"`              // consistency for read operations
	WriteConsistency         string           `json:"writeconsistency"`             // consistency for write operations
	SessionConsistency       string           `json:"session_consistency"`          // consistency that applies to all operations if no read or write consistency is set
	Timeout                  string           `json:"timeout"`                      // connection timeout (default: 600ms)
	ConnectTimeout           string           `json:"connect_timeout"`              // initial connection timeout (default: 600ms)
	KeepAlive                string           `json:"keepalive"`                    // The keepalive period to use default: 0
	NumConns                 int              `json:"numconns"`                     // number of connections per host (default: 2)
	Port                     int              `json:"port"`                         // port to connect to, default: 9042
	NumRetries               int              `json:"num_retries"`                  // number of retries in case of connection timeout
	DisableInitialHostLookup bool             `json:"disableinitialhostlookup"`     // Don't preform ip address discovery on the cluster, just use the Nodes provided
	PreferRPCAddress         bool             `json:"prefer_rpc_address"`           // Prefer to connect to rpc_addresses during cluster discovery
	PoolConfig               gocql.PoolConfig `json:""`
	// Authentication
	Username string `json:"username"`
	Password string `json:"password"`

	// SSL Options
	Ssl CassandraSslConfig `json:"ssl"` // ssl options cert/key/ca ...

	// TestMode affects whether a keyspace creation will be attempted on Cassandra initialization.
	TestMode bool `config:"optional"`
}

type CassandraSslConfig struct {
	Enabled bool
	Options *gocql.SslOptions
}

func (c CassandraConfig) String() string {
	return fmt.Sprintf("CassandraConfig(DataCenter=%v, Nodes=%v, Keyspace=%v, ReadConsistency=%v,"+
		"WriteConsistency=%v, SessionConsistency=%v, NumRetries=%v, TestMode=%v, Timeout=%s, ConnectTimeout=%s)",
		c.DataCenter, c.Nodes, c.Keyspace, c.ReadConsistency,
		c.WriteConsistency, c.SessionConsistency, c.NumRetries, c.TestMode, c.Timeout, c.ConnectTimeout)
}

var NotFound = errors.New("Not found")

func NewCassandra(config CassandraConfig) (Cassandra, error) {
	cluster, err := setDefaults(config)
	if err != nil {
		return nil, errors.Wrap(err, "while setting config defaults")
	}

	// in test mode, create a keyspace if necessary
	if config.TestMode {
		// The policy needs to be removed and then put back to avoid an
		// error with the TokenAwareHostPolicy having Init called twice
		policy := cluster.PoolConfig.HostSelectionPolicy
		cluster.PoolConfig.HostSelectionPolicy = nil
		session, err := cluster.CreateSession()
		if err != nil {
			return nil, errors.Wrap(err, "while creating session")
		}

		query := session.Query(
			fmt.Sprintf(
				`create keyspace if not exists %v
                   with replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`,
				config.Keyspace))

		if err := query.Exec(); err != nil {
			return nil, errors.Wrap(err, "while creating keyspace")
		}

		session.Close()
		cluster.PoolConfig.HostSelectionPolicy = policy
	}

	// switch the keyspace
	cluster.Keyspace = config.Keyspace

	session, err := cluster.CreateSession()
	if err != nil {
		return nil, errors.Wrap(err, "while creating session")
	}

	rcl := gocql.ParseConsistency(config.ReadConsistency)
	wcl := gocql.ParseConsistency(config.WriteConsistency)

	return &cassandra{session, config, rcl, wcl}, nil
}

func (c *cassandra) Close() error {
	c.session.Close()
	return nil
}

func (c *cassandra) Config() CassandraConfig {
	return c.config
}

func (c *cassandra) Session() *gocql.Session {
	return c.session
}

// QueryCtx provides access to the gocql.Query if a user of this library needs to tune some parameters for
// a specific query without modifying the parameters the library was configured with, for example to use
// a consistency level that differs from the configured read/write consistency levels.
func (c *cassandra) QueryCtx(ctx context.Context, consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query {
	return c.session.Query(queryString, queryParams...).WithContext(ctx).Consistency(consistency)
}

// Query same as QueryCtx, but without context.Context.
// Deprecated: Query is deprecated. Use QueryCtx instead.
func (c *cassandra) Query(consistency gocql.Consistency, queryString string, queryParams ...interface{}) *gocql.Query {
	return c.QueryCtx(context.Background(), consistency, queryString, queryParams...)
}

// ExecuteQueryCtx executes a single DML/DDL statement at the configured write consistency level.
func (c *cassandra) ExecuteQueryCtx(ctx context.Context, queryString string, queryParams ...interface{}) error {
	return c.QueryCtx(ctx, c.wcl, queryString, queryParams...).Exec()
}

// ExecuteQuery executes a single DML/DDL statement at the configured write consistency level.
// Deprecated: ExecuteQuery is deprecated. Use ExecuteQueryCtx instead.
func (c *cassandra) ExecuteQuery(queryString string, queryParams ...interface{}) error {
	return c.ExecuteQueryCtx(context.Background(), queryString, queryParams...)
}

// ExecuteBatchCtx executes a batch of DML/DDL statements at the configured write consistency level.
func (c *cassandra) ExecuteBatchCtx(ctx context.Context, batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	count := len(queries)

	// quick sanity check
	if count != len(params) {
		return errors.New("Amount of queries and params does not match")
	}

	batch := c.session.NewBatch(batchType).WithContext(ctx)
	batch.Cons = c.wcl
	for idx := 0; idx < count; idx++ {
		batch.Query(queries[idx], params[idx]...)
	}

	return c.session.ExecuteBatch(batch)
}

// ExecuteBatch executes a batch of DML/DDL statements at the configured write consistency level.
// Deprecated: ExecuteBatch is deprecated. Use ExecuteBatchCtx instead.
func (c *cassandra) ExecuteBatch(batchType gocql.BatchType, queries []string, params [][]interface{}) error {
	return c.ExecuteBatchCtx(context.Background(), batchType, queries, params)
}

// ExecuteUnloggedBatchCtx executes a batch of DML/DDL statements in a non-atomic way at the configured
// write consistency level.
func (c *cassandra) ExecuteUnloggedBatchCtx(ctx context.Context, queries []string, params [][]interface{}) error {
	return c.ExecuteBatchCtx(ctx, gocql.UnloggedBatch, queries, params)
}

// ExecuteUnloggedBatch executes a batch of DML/DDL statements in a non-atomic way at the configured
// write consistency level.
func (c *cassandra) ExecuteUnloggedBatch(queries []string, params [][]interface{}) error {
	return c.ExecuteUnloggedBatchCtx(context.Background(), queries, params)
}

// ScanQueryCtx executes a provided SELECT query at the configured read consistency level.
func (c *cassandra) ScanQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{}) error {
	if err := c.QueryCtx(ctx, c.rcl, queryString, queryParams...).Scan(outParams...); err != nil {
		if err == gocql.ErrNotFound {
			return NotFound
		}
		return err
	}
	return nil
}

// ScanQuery executes a provided SELECT query at the configured read consistency level.
// Deprecated: ScanQuery is deprecated. Use ScanQueryCtx instead.
func (c *cassandra) ScanQuery(queryString string, queryParams []interface{}, outParams ...interface{}) error {
	return c.ScanQueryCtx(context.Background(), queryString, queryParams, outParams...)
}

// ScanCASQueryCtx executes a lightweight transaction (an UPDATE or INSERT statement containing an IF clause)
// at the configured write consistency level.
func (c *cassandra) ScanCASQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{},
) (applied bool, err error) {
	return c.QueryCtx(ctx, c.wcl, queryString, queryParams...).ScanCAS(outParams...)
}

// ScanCASQuery executes a lightweight transaction (an UPDATE or INSERT statement containing an IF clause)
// at the configured write consistency level.
// Deprecated: ScanCASQuery is deprecated. Use ScanCASQueryCtx instead.
func (c *cassandra) ScanCASQuery(queryString string, queryParams []interface{}, outParams ...interface{}) (bool, error) {
	return c.ScanCASQueryCtx(context.Background(), queryString, queryParams, outParams...)
}

// IterQueryCtx consumes row by row of the provided SELECT query executed at the configured read consistency level.
func (c *cassandra) IterQueryCtx(ctx context.Context, queryString string, queryParams []interface{}, outParams ...interface{},
) func() (idx int, hasNext bool, err error) {
	iter := c.QueryCtx(ctx, c.rcl, queryString, queryParams...).Iter()
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

// IterQuery consumes row by row of the provided SELECT query executed at the configured read consistency level.
// Deprecated: IterQuery is deprecated. Use IterQueryCtx instead.
func (c *cassandra) IterQuery(queryString string, queryParams []interface{}, outParams ...interface{}) func() (int, bool, error) {
	return c.IterQueryCtx(context.Background(), queryString, queryParams, outParams...)
}

func TableExistsCtx(ctx context.Context, db Cassandra, table string) (bool, error) {
	var tableName string
	// Only tested with Cassandra 3.11.x
	iter := db.IterQueryCtx(ctx, "SELECT table_name FROM system_schema.tables"+
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

// Deprecated: TableExists is deprecated. Use TableExistsCtx instead.
func TableExists(db Cassandra, table string) (bool, error) {
	return TableExistsCtx(context.Background(), db, table)
}

func WaitForTablesCtx(ctx context.Context, db Cassandra, timeout time.Duration, tables ...string) error {
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
		exists, err := TableExistsCtx(ctx, db, table)
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

// Deprecated: WaitForTables is deprecated. Use WaitForTablesCtx instead.
func WaitForTables(db Cassandra, timeout time.Duration, tables ...string) error {
	return WaitForTablesCtx(context.Background(), db, timeout, tables...)
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

	connectTimeout, err := translateDuration(cfg.ConnectTimeout, 600*time.Millisecond)
	if err != nil {
		return nil, err
	}

	if cfg.Port == 0 {
		cfg.Port = 9042
	}

	if cfg.NumConns == 0 {
		cfg.NumConns = 2
	}

	cluster := gocql.NewCluster(cfg.Nodes...)
	cluster.ProtoVersion = 3
	cluster.CQLVersion = "3.0.0"
	cluster.Timeout = timeout
	cluster.ConnectTimeout = connectTimeout
	cluster.NumConns = cfg.NumConns
	cluster.SocketKeepalive = keepAlive
	cluster.Port = cfg.Port
	cluster.HostFilter = gocql.DataCentreHostFilter(cfg.DataCenter)
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup
	cluster.Consistency = gocql.LocalQuorum
	cluster.PoolConfig = cfg.PoolConfig

	if cfg.SessionConsistency != "" {
		cluster.Consistency = gocql.ParseConsistency(cfg.SessionConsistency)
	}

	if cfg.Username != "" && cfg.Password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	if cfg.NumRetries != 0 {
		cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: cfg.NumRetries}
	}

	if cfg.Ssl.Enabled {
		cluster.SslOpts = cfg.Ssl.Options
	}

	return cluster, nil
}
