package postgresql

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/jackc/pgx/v4"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
)

type tMetricIDMap map[string]int64

// Config for the database
type Config struct {
	CommitSecs      int
	CommitRows      int
	PGWriters       int
	PGParsers       int
	PartitionScheme string
}

var promSamples = list.New()

// QueueMutex is used thread safe operations on promSamples list object.
var QueueMutex sync.Mutex

// vMetricMaxID is the highest metric_id known to this process (loaded from
// metric_labels at start, then advanced for every new series). New series get
// vMetricMaxID+1: ids in the table are not dense, so len(map)+1 collided with
// existing ids after a restart and wrote samples under another series' id.
var vMetricMaxID int64

// maxBufferedRows bounds what is kept in memory while the database is
// unreachable; beyond it the oldest batch is dropped and logged.
const maxBufferedRows = 1000000

var vMetricIDMapMutex sync.Mutex
var vMetricIDMap tMetricIDMap

// PGWriter - Threaded writer
type PGWriter struct {
	DB          *pgx.Conn
	id          int
	KeepRunning bool
	Running     bool

	valueRows [][]interface{}
	labelRows [][]interface{}

	PGWriterMutex   sync.Mutex
	partitionScheme string
	logger          log.Logger
}

// PGParser - Threaded parser
type PGParser struct {
	id          int
	KeepRunning bool
	Running     bool

	lastPartitionTS time.Time
	valueRows       [][]interface{}
}

// partitionsReady remembers the days whose partitions exist (per process).
var partitionsReady = map[string]bool{}

// ensurePartitions creates the partitions for every day present in rows
// before they are copied, so a sample never meets a missing partition.
func (c *PGWriter) ensurePartitions(rows [][]interface{}) error {
	for _, r := range rows {
		t, ok := r[1].(time.Time)
		if !ok {
			continue
		}
		day := t.UTC().Format("20060102")
		if partitionsReady[day] {
			continue
		}
		if err := c.setupPgPartitions(c.partitionScheme, t.UTC()); err != nil {
			return err
		}
		partitionsReady[day] = true
	}
	return nil
}

// RunPGParser starts the client and listens for a shutdown call.
func (p *PGParser) RunPGParser(tid int, partitionScheme string, c *PGWriter) {
	var samples *model.Samples
	p.id = tid
	level.Info(c.logger).Log(fmt.Sprintf("bgparser%d", p.id), "Started")
	p.Running = true
	p.KeepRunning = true

	// Loop that runs forever
	for p.KeepRunning {
		samples = Pop()
		if samples != nil {
			for _, sample := range *samples {
				sMetric := metricString(sample.Metric)
				ts := time.Unix(sample.Timestamp.Unix(), 0)
				milliseconds := sample.Timestamp.UnixNano() / 1000000
				_ = ts // partitions are created by the writer (ensurePartitions), which owns the connection
				vMetricIDMapMutex.Lock()
				id, ok := vMetricIDMap[sMetric]

				if !ok {
					vMetricMaxID++
					var nextId int64 = vMetricMaxID
					vMetricIDMap[sMetric] = nextId
					i := strings.Index(sMetric, "{")
					jsonbMap := make(map[string]interface{})
					json.Unmarshal([]byte(sMetric[i:]), &jsonbMap)
					c.labelRows = append(c.labelRows, []interface{}{int64(nextId), sMetric[:i], sMetric, jsonbMap})
					id = nextId
				}
				vMetricIDMapMutex.Unlock()
				p.valueRows = append(p.valueRows, []interface{}{int64(id), toTimestamp(milliseconds), float64(sample.Value)})
			}
			vMetricIDMapMutex.Lock()
			c.valueRows = append(c.valueRows, p.valueRows...)
			p.valueRows = nil
			vMetricIDMapMutex.Unlock()
			runtime.GC()
		}
		time.Sleep(10 * time.Millisecond)
	}
	level.Info(c.logger).Log(fmt.Sprintf("bgparser%d", p.id), "Shutdown")
	p.Running = false
}

// PGParserShutdown is a graceful shutdown
func (p *PGParser) PGParserShutdown() {
	p.KeepRunning = false
}

// RunPGWriter starts the client and listens for a shutdown call.
func (c *PGWriter) RunPGWriter(l log.Logger, tid int, commitSecs int, commitRows int, Parsers int, partitionScheme string) {
	c.logger = l
	c.id = tid
	c.partitionScheme = partitionScheme
	period := commitSecs * 1000
	var err error
	var parser [20]PGParser
	c.DB, err = pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		level.Error(c.logger).Log("err", err)
		os.Exit(1)
	}
	if c.id == 0 {
		c.setupPgPrometheus()
		_ = c.setupPgPartitions(partitionScheme, time.Now())
	}
	level.Info(c.logger).Log(fmt.Sprintf("bgwriter%d", c.id), fmt.Sprintf("Starting %d Parsers", Parsers))
	for p := 0; p < Parsers; p++ {
		go parser[p].RunPGParser(p, partitionScheme, c)
		defer parser[p].PGParserShutdown()
	}
	level.Info(c.logger).Log(fmt.Sprintf("bgwriter%d", c.id), "Started")
	c.Running = true
	c.KeepRunning = true
	// Loop that runs forever
	for c.KeepRunning {
		values, labels := c.pending()
		if (period <= 0 && (values > 0 || labels > 0)) || (values > commitRows) {
			c.PGWriterSave()
			period = commitSecs * 1000
		} else {
			time.Sleep(10 * time.Millisecond)
			period -= 10
		}
	}
	c.PGWriterSave()
	level.Info(c.logger).Log(fmt.Sprintf("bgwriter%d", c.id), "Shutdown")
	c.Running = false
}

// PGWriterShutdown - Set shutdown flag for graceful shutdown
func (c *PGWriter) PGWriterShutdown() {
	c.KeepRunning = false
}

// PGWriterSave save data to DB. The batch is taken under the parsers' mutex;
// on a broken connection the writer reconnects (DATABASE_URL may list several
// hosts with target_session_attrs=read-write, so it follows a promoted
// primary) and retries the same rows. A batch that still fails is put back
// and retried at the next commit instead of being dropped.
func (c *PGWriter) PGWriterSave() {
	begin := time.Now()
	c.PGWriterMutex.Lock()
	defer c.PGWriterMutex.Unlock()
	vMetricIDMapMutex.Lock()
	labelRows, valueRows := c.labelRows, c.valueRows
	c.labelRows, c.valueRows = nil, nil
	vMetricIDMapMutex.Unlock()
	lblCount, rowCount := int64(len(labelRows)), int64(len(valueRows))
	if lblCount == 0 && rowCount == 0 {
		return
	}
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if c.DB == nil || c.DB.IsClosed() {
			if err = c.reconnect(); err != nil {
				level.Error(c.logger).Log("msg", "reconnect failed", "attempt", attempt, "err", err)
				time.Sleep(time.Duration(attempt) * 2 * time.Second)
				continue
			}
		}
		if len(labelRows) > 0 {
			var n int64
			n, err = c.DB.CopyFrom(context.Background(), pgx.Identifier{"metric_labels"}, []string{"metric_id", "metric_name", "metric_name_label", "metric_labels"}, pgx.CopyFromRows(labelRows))
			if err != nil {
				level.Error(c.logger).Log("msg", "COPY failed for metric_labels", "attempt", attempt, "err", err)
				if c.DB.IsClosed() {
					continue
				}
				// Not a connection problem: the values below would point at ids
				// without a label row. Keep both and retry later.
				break
			}
			if n == lblCount {
				labelRows = nil
			}
		}
		if err = c.ensurePartitions(valueRows); err != nil {
			level.Error(c.logger).Log("msg", "creating partitions failed", "attempt", attempt, "err", err)
			if c.DB.IsClosed() {
				continue
			}
			break
		}
		var n int64
		n, err = c.DB.CopyFrom(context.Background(), pgx.Identifier{"metric_values"}, []string{"metric_id", "metric_time", "metric_value"}, pgx.CopyFromRows(valueRows))
		if err != nil {
			level.Error(c.logger).Log("msg", "COPY failed for metric_values", "attempt", attempt, "err", err)
			if c.DB.IsClosed() {
				continue
			}
			break
		}
		if n != rowCount {
			level.Error(c.logger).Log("msg", "All rows not copied metric_values", "copied", n, "want", rowCount)
		}
		valueRows = nil
		break
	}
	if len(labelRows) > 0 || len(valueRows) > 0 {
		vMetricIDMapMutex.Lock()
		c.labelRows = append(labelRows, c.labelRows...)
		c.valueRows = append(valueRows, c.valueRows...)
		if len(c.valueRows) > maxBufferedRows {
			drop := len(c.valueRows) - maxBufferedRows
			c.valueRows = c.valueRows[drop:]
			level.Error(c.logger).Log("msg", "buffer full, dropped oldest samples", "dropped", drop)
		}
		vMetricIDMapMutex.Unlock()
		level.Error(c.logger).Log("msg", "batch kept for retry", "labels", len(labelRows), "values", len(valueRows), "err", err)
		return
	}
	duration := time.Since(begin).Seconds()
	level.Info(c.logger).Log("metric", fmt.Sprintf("BGWriter%d: Processed samples count,%d, duration,%v", c.id, rowCount+lblCount, duration))
}

// pending returns the queued value and label rows under the parsers' mutex.
func (c *PGWriter) pending() (values, labels int) {
	vMetricIDMapMutex.Lock()
	defer vMetricIDMapMutex.Unlock()
	return len(c.valueRows), len(c.labelRows)
}

// reconnect replaces a broken connection with a new one to DATABASE_URL.
func (c *PGWriter) reconnect() error {
	if c.DB != nil {
		_ = c.DB.Close(context.Background())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		c.DB = nil
		return err
	}
	c.DB = db
	level.Info(c.logger).Log("msg", "reconnected to PostgreSQL", "host", db.Config().Host)
	return nil
}

// Push - Push element at then end of list
func Push(samples *model.Samples) {
	QueueMutex.Lock()
	promSamples.PushBack(samples)
	QueueMutex.Unlock()
}

// Pop - Pop first element from list
func Pop() *model.Samples {
	QueueMutex.Lock()
	defer QueueMutex.Unlock()
	p := promSamples.Front()
	if p != nil {
		return promSamples.Remove(p).(*model.Samples)
	}
	return nil
}

// Client - struct to hold critical values
type Client struct {
	logger log.Logger
	DB     *pgx.Conn
	cfg    *Config
}

// NewClient creates a new PostgreSQL client
func NewClient(logger log.Logger, cfg *Config) *Client {
	if logger == nil {
		logger = log.NewNopLogger()
	}

	conn1, err := pgx.Connect(context.Background(), os.Getenv("DATABASE_URL"))
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: Unable to connect to database using DATABASE_URL=", os.Getenv("DATABASE_URL"))
		os.Exit(1)
	}

	client := &Client{
		logger: logger,
		DB:     conn1,
		cfg:    cfg,
	}

	return client
}

func (c *PGWriter) setupPgPrometheus() error {
	level.Info(c.logger).Log("msg", "creating tables")

	_, err := c.DB.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_labels ( metric_id BIGINT PRIMARY KEY, metric_name TEXT NOT NULL, metric_name_label TEXT NOT NULL, metric_labels jsonb, UNIQUE(metric_name, metric_labels) )")
	if err != nil {
		return err
	}

	_, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_labels_labels_idx ON metric_labels USING GIN (metric_labels)")
	if err != nil {
		return err
	}

	_, err = c.DB.Exec(context.Background(), "CREATE TABLE IF NOT EXISTS metric_values (metric_id BIGINT, metric_time TIMESTAMPTZ, metric_value FLOAT8 ) PARTITION BY RANGE (metric_time)")
	if err != nil {
		return err
	}

	_, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_id_time_idx on metric_values USING btree (metric_id, metric_time DESC)")
	if err != nil {
		return err
	}

	_, err = c.DB.Exec(context.Background(), "CREATE INDEX IF NOT EXISTS metric_values_time_idx  on metric_values USING btree (metric_time DESC)")
	if err != nil {
		return err
	}

	vMetricIDMapMutex.Lock()
	defer vMetricIDMapMutex.Unlock()
	vMetricIDMap = make(tMetricIDMap)
	rows, err1 := c.DB.Query(context.Background(), "SELECT metric_name_label, metric_id from metric_labels")

	if err1 != nil {
		rows.Close()
		level.Info(c.logger).Log("msg", "Error reading metric_labels")
		return err
	}

	for rows.Next() {
		var (
			metricNameLabel string
			metricID        int64
		)
		err := rows.Scan(&metricNameLabel, &metricID)

		if err != nil {
			rows.Close()
			level.Info(c.logger).Log("msg", "Error scaning metric_labels")
			return err
		}
		vMetricIDMap[metricNameLabel] = metricID
		if metricID > vMetricMaxID {
			vMetricMaxID = metricID
		}
	}
	level.Info(c.logger).Log("msg", fmt.Sprintf("%d Rows Loaded in map: ", len(vMetricIDMap)), "max_metric_id", vMetricMaxID)
	rows.Close()

	return nil
}

func (c *PGWriter) setupPgPartitions(partitionScheme string, lastPartitionTS time.Time) error {
	sDate := lastPartitionTS
	eDate := sDate
	if partitionScheme == "daily" {
		level.Info(c.logger).Log("msg", "Creating partition, daily")
		_, err := c.DB.Exec(context.Background(), fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00')", sDate.Format("20060102"), sDate.Format("2006-01-02"), eDate.AddDate(0, 0, 1).Format("2006-01-02")))
		if err != nil {
			return err
		}
	} else if partitionScheme == "hourly" {
		sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS metric_values_%s PARTITION OF metric_values FOR VALUES FROM ('%s 00:00:00') TO ('%s 00:00:00') PARTITION BY RANGE (metric_time);", sDate.Format("20060102"), sDate.Format("2006-01-02"), eDate.AddDate(0, 0, 1).Format("2006-01-02"))
		var h int
		for h = 0; h < 23; h++ {
			sql = fmt.Sprintf("%s CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s %02d:00:00');", sql, sDate.Format("20060102"), h, sDate.Format("20060102"), sDate.Format("2006-01-02"), h, eDate.Format("2006-01-02"), h+1)
		}
		level.Info(c.logger).Log("msg", "Creating partition, hourly")
		_, err := c.DB.Exec(context.Background(), fmt.Sprintf("%s CREATE TABLE IF NOT EXISTS metric_values_%s_%02d PARTITION OF metric_values_%s FOR VALUES FROM ('%s %02d:00:00') TO ('%s 00:00:00');", sql, sDate.Format("20060102"), h, sDate.Format("20060102"), sDate.Format("2006-01-02"), h, eDate.AddDate(0, 0, 1).Format("2006-01-02")))
		if err != nil {
			return err
		}
	}
	return nil
}

func metricString(m model.Metric) string {
	metricName, hasName := m[model.MetricNameLabel]
	numLabels := len(m) - 1
	if !hasName {
		numLabels = len(m)
	}
	labelStrings := make([]string, 0, numLabels)
	for label, value := range m {
		if label != model.MetricNameLabel {
			labelStrings = append(labelStrings, fmt.Sprintf("\"%s\": %q", label, value))
		}
	}

	switch numLabels {
	case 0:
		if hasName {
			return string(metricName)
		}
		return "{}"
	default:
		sort.Strings(labelStrings)
		return fmt.Sprintf("%s{%s}", metricName, strings.Join(labelStrings, ", "))
	}
}

// Write implements the Writer interface and writes metric samples to the database
func (c *Client) Write(samples model.Samples) error {
	Push(&samples)
	return nil
}

type sampleLabels struct {
	JSON        []byte
	Map         map[string]string
	OrderedKeys []string
}

func createOrderedKeys(m *map[string]string) []string {
	keys := make([]string, 0, len(*m))
	for k := range *m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// Close - Close database connections
func (c *Client) Close() {
	if c.DB != nil {
		if err1 := c.DB.Close(context.Background()); err1 != nil {
			level.Error(c.logger).Log("msg", err1.Error())
		}
	}
}

func (l *sampleLabels) Scan(value interface{}) error {
	if value == nil {
		l = &sampleLabels{}
		return nil
	}

	switch t := value.(type) {
	case []uint8:
		m := make(map[string]string)
		err := json.Unmarshal(t, &m)

		if err != nil {
			return err
		}

		*l = sampleLabels{
			JSON:        t,
			Map:         m,
			OrderedKeys: createOrderedKeys(&m),
		}
		return nil
	}
	return fmt.Errorf("invalid labels value %s", reflect.TypeOf(value))
}

func (l sampleLabels) String() string {
	return string(l.JSON)
}

func (l sampleLabels) key(extra string) string {
	// 0xff cannot cannot occur in valid UTF-8 sequences, so use it
	// as a separator here.
	separator := "\xff"
	pairs := make([]string, 0, len(l.Map)+1)
	pairs = append(pairs, extra+separator)

	for _, k := range l.OrderedKeys {
		pairs = append(pairs, k+separator+l.Map[k])
	}
	return strings.Join(pairs, separator)
}

func (l *sampleLabels) len() int {
	return len(l.OrderedKeys)
}

// Read implements the Reader interface and reads metrics samples from the database
func (c *Client) Read(req *prompb.ReadRequest) (*prompb.ReadResponse, error) {

	fmt.Printf("READ req.Queries: %v\n", req.Queries)
	labelsToSeries := map[string]*prompb.TimeSeries{}

	for _, q := range req.Queries {
		command, err := c.buildCommand(q)

		if err != nil {
			return nil, err
		}

		level.Debug(c.logger).Log("msg", "Executed query", "query", command)

		rows, err := c.DB.Query(context.Background(), command)

		if err != nil {
			rows.Close()
			return nil, err
		}

		for rows.Next() {
			var (
				value  float64
				name   string
				labels sampleLabels
				time   time.Time
			)
			err := rows.Scan(&time, &name, &value, &labels)

			if err != nil {
				rows.Close()
				return nil, err
			}

			key := labels.key(name)
			ts, ok := labelsToSeries[key]

			if !ok {
				labelPairs := make([]prompb.Label, 0, labels.len()+1)
				labelPairs = append(labelPairs, prompb.Label{
					Name:  model.MetricNameLabel,
					Value: name,
				})

				for _, k := range labels.OrderedKeys {
					labelPairs = append(labelPairs, prompb.Label{
						Name:  k,
						Value: labels.Map[k],
					})
				}

				ts = &prompb.TimeSeries{
					Labels:  labelPairs,
					Samples: make([]prompb.Sample, 0, 100),
				}
				labelsToSeries[key] = ts
			}

			ts.Samples = append(ts.Samples, prompb.Sample{
				Timestamp: time.UnixNano() / 1000000,
				Value:     value,
			})
		}

		err = rows.Err()
		rows.Close()

		if err != nil {
			return nil, err
		}
	}

	resp := prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: make([]*prompb.TimeSeries, 0, len(labelsToSeries)),
			},
		},
	}
	for _, ts := range labelsToSeries {
		resp.Results[0].Timeseries = append(resp.Results[0].Timeseries, ts)
	}

	level.Debug(c.logger).Log("msg", "Returned response", "#timeseries", len(labelsToSeries))

	return &resp, nil
}

// HealthCheck implements the healtcheck interface
func (c *Client) HealthCheck() error {
	rows, err := c.DB.Query(context.Background(), "SELECT 1")
	defer rows.Close()
	if err != nil {
		level.Debug(c.logger).Log("msg", "Health check error", "err", err)
		return err
	}

	return nil
}

func toTimestamp(milliseconds int64) time.Time {
	sec := milliseconds / 1000
	nsec := (milliseconds - (sec * 1000)) * 1000000
	return time.Unix(sec, nsec).UTC()
}

func (c *Client) buildQuery(q *prompb.Query) (string, error) {
	matchers := make([]string, 0, len(q.Matchers))
	labelEqualPredicates := make(map[string]string)

	for _, m := range q.Matchers {
		escapedName := escapeValue(m.Name)
		escapedValue := escapeValue(m.Value)

		if m.Name == model.MetricNameLabel {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					matchers = append(matchers, fmt.Sprintf("(l.metric_name IS NULL OR name = '')"))
				} else {
					matchers = append(matchers, fmt.Sprintf("l.metric_name = '%s'", escapedValue))
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("l.metric_name != '%s'", escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("l.metric_name ~ '%s'", anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("l.metric_name !~ '%s'", anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown metric name match type %v", m.Type)
			}
		} else {
			switch m.Type {
			case prompb.LabelMatcher_EQ:
				if len(escapedValue) == 0 {
					// From the PromQL docs: "Label matchers that match
					// empty label values also select all time series that
					// do not have the specific label set at all."
					matchers = append(matchers, fmt.Sprintf("((l.metric_labels ? '%s') = false OR (l.metric_labels->>'%s' = ''))",
						escapedName, escapedName))
				} else {
					labelEqualPredicates[escapedName] = escapedValue
				}
			case prompb.LabelMatcher_NEQ:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' != '%s'", escapedName, escapedValue))
			case prompb.LabelMatcher_RE:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' ~ '%s'", escapedName, anchorValue(escapedValue)))
			case prompb.LabelMatcher_NRE:
				matchers = append(matchers, fmt.Sprintf("l.metric_labels->>'%s' !~ '%s'", escapedName, anchorValue(escapedValue)))
			default:
				return "", fmt.Errorf("unknown match type %v", m.Type)
			}
		}
	}
	equalsPredicate := ""

	if len(labelEqualPredicates) > 0 {
		labelsJSON, err := json.Marshal(labelEqualPredicates)

		if err != nil {
			return "", err
		}
		equalsPredicate = fmt.Sprintf(" AND l.metric_labels @> '%s'", labelsJSON)
	}

	matchers = append(matchers, fmt.Sprintf("v.metric_time >= '%v'", toTimestamp(q.StartTimestampMs).Format(time.RFC3339)))
	matchers = append(matchers, fmt.Sprintf("v.metric_time <= '%v'", toTimestamp(q.EndTimestampMs).Format(time.RFC3339)))

	return fmt.Sprintf("SELECT v.metric_time, l.metric_name, v.metric_value, l.metric_labels FROM metric_values v, metric_labels l WHERE l.metric_id = v.metric_id and %s %s ORDER BY v.metric_time",
		strings.Join(matchers, " AND "), equalsPredicate), nil
}

func (c *Client) buildCommand(q *prompb.Query) (string, error) {
	return c.buildQuery(q)
}

func escapeValue(str string) string {
	return strings.Replace(str, `'`, `''`, -1)
}

// anchorValue adds anchors to values in regexps since PromQL docs
// states that "Regex-matches are fully anchored."
func anchorValue(str string) string {
	l := len(str)

	if l == 0 || (str[0] == '^' && str[l-1] == '$') {
		return str
	}

	if str[0] == '^' {
		return fmt.Sprintf("%s$", str)
	}

	if str[l-1] == '$' {
		return fmt.Sprintf("^%s", str)
	}

	return fmt.Sprintf("^%s$", str)
}

// Name identifies the client as a PostgreSQL client.
func (c Client) Name() string {
	return "PostgreSQL"
}
