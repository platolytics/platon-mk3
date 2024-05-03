package platon

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	sb "github.com/huandu/go-sqlbuilder"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/platolytics/platon-mk3/pkg/db/clickhouse"
	"github.com/prometheus/client_golang/api"
	v1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
)

type Platon struct {
	Cubes         Cubes
	Database      clickhouse.Clickhouse
	StartTime     time.Time
	EndTime       time.Time
	Client        api.Client
	PrometheusUrl string
	ctx           context.Context
}

type Metric struct {
	Name       string
	Dimensions []string
}

func WatchCubes(clickhouse clickhouse.Clickhouse, cubes Cubes, prometheusUrl string) {
	p := NewPlaton(prometheusUrl)
	p.Cubes = cubes
	p.Database = clickhouse

	p.WatchCubes()
}

func (p *Platon) WatchCubes() {
	for {
		for _, cube := range p.Cubes.Cubes {
			if cube.LastUpdate.Add(cube.ScrapeInterval).After(time.Now()) {
				continue
			}

			fmt.Printf("Updating cube %s.\n", cube.Name)
			p.UpdateCube(cube)
			cube.LastUpdate = time.Now()
		}
		time.Sleep(1 * time.Minute)
	}
}

func NewPlaton(prometheusUrl string) *Platon {
	p := Platon{
		PrometheusUrl: prometheusUrl,
		ctx:           context.Background(),
	}

	p.SetQueryTimes()
	// start prometheus client
	client, err := p.getPromClient()
	if err != nil {
		log.Fatal(err)
	}
	p.Client = client
	return &p
}

func (p *Platon) UpdateCube(cube Cube) {

	tables := []Table{}

	start := time.Now().Add(-1 * time.Hour)
	if !cube.LastUpdate.IsZero() {
		start = cube.LastUpdate.Add(1 * time.Minute)
	}
	end := time.Now()
	for _, query := range cube.Queries {
		fmt.Printf("Querying prometheus: %s\n", query.PromQL)

		queryResult, err := p.GetSamples(string(query.PromQL), start, end)
		if err != nil {
			panic(err)
		}

		table, err := MetricsToTable(query, queryResult)
		if err != nil {
			panic(err)
		}

		tables = append(tables, table)
		table.PrettyPrint(10)

		err = p.EnsureTable(table)
		if err != nil {
			panic(err)
		}

		err = p.InsertData(table)
		if err != nil {
			panic(fmt.Errorf("failed to add data to table %s: %v", table.Name, err))
		}
	}
	err := p.CreateView(cube, tables)
	if err != nil {
		panic(err)
	}
}

func (p *Platon) CreateView(cube Cube, tables []Table) error {

	uniqueColumns := []string{}
	columnsWithAlias := []string{}
	for i, t := range tables {
		for _, c := range t.GetColumns() {
			if !slices.Contains(uniqueColumns, c.Name) {
				uniqueColumns = append(uniqueColumns, c.Name)
				columnsWithAlias = append(columnsWithAlias, fmt.Sprintf("\"T%d\".\"%s\" \"%s\"", i, c.Name, c.Name))
			}
		}
	}

	ctx := context.Background()

	viewSqlBuilder := sb.ClickHouse.NewSelectBuilder()
	viewSqlBuilder = viewSqlBuilder.Select(columnsWithAlias...)

	for i, table := range tables {
		if i == 0 {
			viewSqlBuilder = viewSqlBuilder.From(table.Name + " T0")
			continue
		}
		onExpr := []string{}
		joinCols := append(cube.JoinedLabels, "Time")
		for _, joinCol := range joinCols {
			onExpr = append(onExpr, fmt.Sprintf("T%d.%s=T%d.%s", i-1, joinCol, i, joinCol))
		}
		viewSqlBuilder = viewSqlBuilder.JoinWithOption(sb.InnerJoin, table.Name+" T"+strconv.Itoa(i), onExpr...)
	}
	selectSql, _ := viewSqlBuilder.Build()
	viewSql := "CREATE OR REPLACE VIEW " + cube.Name + " AS " + selectSql

	fmt.Println("Executing SQL: " + viewSql)

	err := p.Database.Connection.Exec(ctx, viewSql)
	if err != nil {
		return fmt.Errorf("failed to CREATE or UPDATE cube view: %w", err)
	}

	return nil
}

func (p *Platon) EnsureTable(table Table) error {
	exists, err := p.TableExists(table)
	if err != nil {
		return fmt.Errorf("failed to figure out if table %s exists: %v", table.Name, err)
	}
	if exists {
		err = p.EnsureColumns(table)
		if err != nil {
			return fmt.Errorf("failed to update table %s: %v", table.Name, err)
		}
		return nil
	}
	err = p.CreateTable(table)
	if err != nil {
		return fmt.Errorf("failed to create table %s: %v", table.Name, err)
	}
	return nil
}

func (p *Platon) TableExists(table Table) (bool, error) {
	sql := fmt.Sprintf("EXISTS TABLE %s", table.Name)
	row := p.Database.Connection.QueryRow(p.ctx, sql)
	var existsCol uint8
	if err := row.Scan(&existsCol); err != nil {
		return false, fmt.Errorf("failed to query result row of sql '%s': %w", sql, err)
	}

	return existsCol == 1, nil
}

func (p *Platon) EnsureColumns(table Table) error {

	sql := fmt.Sprintf("DESCRIBE TABLE %s", table.Name)
	fmt.Printf("Executing sql: %s", sql)
	rows, err := p.Database.Connection.Query(p.ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to query table columns with sql '%s': %w", sql, err)
	}
	columnNames := []string{}
	for rows.Next() {
		var (
			columnName        string
			columnType        string
			defaultType       string
			defaultExpression string
			comment           string
			codec             string
			ttl               string
		)
		err = rows.Scan(&columnName, &columnType, &defaultType, &defaultExpression, &comment, &codec, &ttl)
		if err != nil {
			return fmt.Errorf("failed to scan table columns from sql '%s': %w", sql, err)
		}
		fmt.Println("---")
		fmt.Printf("Name: %s\n", columnName)
		fmt.Printf("Type: %s\n", columnType)
		fmt.Printf("defaultype: %s\n", defaultType)
		fmt.Printf("defaultexpression: %s\n", defaultExpression)
		fmt.Printf("Comment: %s\n", comment)
		fmt.Printf("Codec: %s\n", codec)
		fmt.Printf("ttl: %s\n", ttl)
		columnNames = append(columnNames, columnName)
	}

	fmt.Printf("%d columns found in table %s: %v\n", len(columnNames), table.Name, columnNames)

	for _, expectedCol := range table.GetColumns() {
		if slices.Contains(columnNames, expectedCol.Name) {
			continue
		}
		sql := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table.Name, expectedCol.Name, expectedCol.DataType)
		fmt.Printf("Executing sql: %s", sql)
		err := p.Database.Connection.Exec(p.ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to update cube table %s with SQL '%s': %w", table.Name, sql, err)
		}

	}

	return nil
}

func (p *Platon) CreateTable(table Table) error {
	cols := table.GetColumns()
	columns := ""
	for _, c := range cols {
		columns = columns + c.Name + " " + c.DataType + ","
	}
	columns = columns[:len(columns)-1]
	sql := fmt.Sprintf("CREATE TABLE %s (%s) PRIMARY KEY(Time)", table.Name, columns)
	fmt.Println(sql)

	err := p.Database.Connection.Exec(p.ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create cube table: %w", err)
	}
	return nil
}
func (p *Platon) InsertData(table Table) error {
	cols := table.GetColumns()
	for i := 0; i < len(table.Rows)/100; i++ {
		batch, err := p.Database.Connection.PrepareBatch(p.ctx, "INSERT INTO "+table.Name+" ("+strings.Join(table.GetQuotedColumnNames(), ", ")+")")
		if err != nil {
			return err
		}
		for j := i * 100; j < len(table.Rows) && j < (i+1)*100; j++ {
			//fmt.Printf("Adding %v", table.Rows[i].GetOrderedValues(cols)...)
			err = batch.Append(table.Rows[j].GetOrderedValues(cols)...)
			if err != nil {
				return fmt.Errorf("failed to add row to batch: %w", err)
			}
		}
		err = batch.Send()
		if err != nil {
			return fmt.Errorf("failed to execute batch batch: %w", err)
		}
	}
	return nil
}

var promClient api.Client
var promClientInitialized bool = false

func (p *Platon) getPromClient() (api.Client, error) {
	if promClientInitialized {
		return promClient, nil
	}

	var err error
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		},
	}
	promClient, err = api.NewClient(api.Config{
		Address: p.PrometheusUrl,
		Client:  &httpClient,
	})

	if err != nil {
		return nil, fmt.Errorf("error creating client: %v", err)
	}
	promClientInitialized = true

	return promClient, nil
}

func (p *Platon) GetMetrics(metricsFilter ...string) ([]Metric, error) {
	v1api := v1.NewAPI(p.Client)
	labels, warnings, err := v1api.LabelValues(context.Background(), "__name__", []string{}, p.StartTime, p.EndTime)
	// Always log the warnings even if errors cause crash
	if len(warnings) > 0 {
		fmt.Printf("Warnings: %v\n", warnings)
	}
	if err != nil {
		return nil, err
	}
	metrics := []Metric{}
	for i := range labels {
		metricName := string(labels[i])
		if len(metricsFilter) > 0 && !slices.Contains(metricsFilter, metricName) {
			continue
		}
		//Query metric to identify dimensions
		samples, err := p.GetSamples(metricName, time.Now().Add(-1*time.Hour), time.Now())
		if err != nil {
			return nil, fmt.Errorf("failed to query metric %s: %w", metricName, err)
		}
		metric := Metric{
			Name: metricName,
		}
		matrix := samples.(model.Matrix)
		for _, sampleStream := range matrix {
			for label, _ := range sampleStream.Metric {
				if string(label) == "__name__" {
					continue
				}
				if !slices.Contains(metric.Dimensions, string(label)) {
					metric.Dimensions = append(metric.Dimensions, string(label))
				}
			}
		}

		metrics = append(metrics, metric)
	}
	return metrics, nil
}

func GenerateCube(cubeName string, metricNames []string, prometheusUrl string) Cubes {
	cubes := Cubes{}
	p := NewPlaton(prometheusUrl)
	metrics, err := p.GetMetrics(metricNames...)
	if err != nil {
		panic(err)
	}

	cube := Cube{
		Name:           cubeName,
		Description:    "My Cube",
		Ttl:            1 * time.Hour,
		ScrapeInterval: 1 * time.Minute,
	}
	commonLabels := []string{}
	for i, metric := range metrics {
		query := Query{
			Value:  metric.Name,
			Name:   metric.Name,
			PromQL: metric.Name,
		}
		cube.Queries = append(cube.Queries, query)
		if i == 0 {
			commonLabels = metric.Dimensions
			continue
		}
		newCommonLabels := []string{}
		for _, label := range commonLabels {
			if slices.Contains(metric.Dimensions, label) {
				newCommonLabels = append(newCommonLabels, label)
			}
		}
		commonLabels = newCommonLabels
	}
	cube.JoinedLabels = commonLabels
	cubes.Cubes = []Cube{cube}

	return cubes
}

func PrintDimensions(prometheusUrl string) {
	p := NewPlaton(prometheusUrl)
	metrics, err := p.GetMetrics()
	if err != nil {
		panic(err)
	}
	allDimensions := []string{}
	for _, metric := range metrics {
		allDimensions = append(allDimensions, metric.Dimensions...)
	}
	slices.Sort(allDimensions)
	allDimensions = slices.Compact(allDimensions)
	fmt.Println("All Dimensions:")
	for _, dim := range allDimensions {
		fmt.Println(dim)
	}
	fmt.Printf("%d dimensions found in Prometheus instance.\n", len(allDimensions))
}

func PrintMetrics(dimensionFilter []string, prometheusUrl string) {
	p := NewPlaton(prometheusUrl)

	metrics, err := p.GetMetrics()
	if err != nil {
		panic(err)
	}

	tab := table.NewWriter()
	tab.SetOutputMirror(os.Stdout)
	header := table.Row{"Metric", "Dimensions"}

	tab.AppendHeader(header)
	foundMetrics := 0
metricLoop:
	for _, metric := range metrics {
		for _, filter := range dimensionFilter {
			if !slices.Contains(metric.Dimensions, filter) {
				continue metricLoop
			}
		}
		foundMetrics = foundMetrics + 1
		row := table.Row{metric.Name, strings.Join(metric.Dimensions, ", ")}
		tab.AppendRow(row)
	}
	fmt.Println("All Metrics:")
	tab.Render()

	fmt.Printf("listing %d metrics out of %d found in Prometheus instance.\n", foundMetrics, len(metrics))
}

func (p *Platon) GetSamples(metric string, start, end time.Time) (model.Value, error) {
	v1api := v1.NewAPI(p.Client)

	result, warnings, err := v1api.QueryRange(context.TODO(), metric, v1.Range{Start: start, End: end, Step: 1 * time.Minute}, v1.WithTimeout(5*time.Second))
	// Always log the warnings even if errors cause crash
	if len(warnings) > 0 {
		fmt.Printf("Warnings: %v\n", warnings)
	}
	if err != nil {
		return nil, fmt.Errorf("error querying Prometheus: %w", err)
	}

	return result, nil
}

// SetQueryTimes sets startTime to now and endTime one hour in the past
func (p *Platon) SetQueryTimes() {
	p.StartTime = time.Now().Add(-1 * time.Hour)
	p.EndTime = time.Now()
}

func MetricsToTable(query Query, queryResult model.Value) (Table, error) {
	table := Table{
		Name:       query.Name,
		Dimensions: []string{},
		Rows:       []*Row{},
	}

	table.addQueryResult(query, queryResult)
	fmt.Printf("Rows added to internal table: %d\n", len(table.Rows))
	return table, nil
}
