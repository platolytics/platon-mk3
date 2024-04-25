package platon

import (
	"context"
	"flag"
	"fmt"
	"log"
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
	Cubes     Cubes
	Database  clickhouse.Clickhouse
	StartTime time.Time
	EndTime   time.Time
	Client    api.Client
}

type Metric struct {
	Name       string
	Dimensions []string
}

func WatchCubes(clickhouse clickhouse.Clickhouse, cubes Cubes) {
	p := NewPlaton()
	p.Cubes = cubes
	p.Database = clickhouse

	p.WatchCubes()
}

func (p *Platon) WatchCubes() {
	//for {
	for _, cube := range p.Cubes.Cubes {
		fmt.Printf("Updating cube %s.\n", cube.Name)
		p.UpdateCube(cube)
	}
	//}
}

func NewPlaton() *Platon {
	p := Platon{}

	p.SetQueryTimes()
	// start prometheus client
	client, err := GetPromClient()
	if err != nil {
		log.Fatal(err)
	}
	p.Client = client
	return &p
}

func (p *Platon) UpdateCube(cube Cube) {
	var queryResults []model.Value

	tables := []Table{}

	for _, query := range cube.Queries {
		fmt.Printf("Querying prometheus: %s\n", query.PromQL)
		queryResult, err := p.GetSamples(string(query.PromQL))
		if err != nil {
			panic(err)
		}
		queryResults = append(queryResults, queryResult)

		table, err := MetricsToTable(query.Name, queryResults, cube)
		if err != nil {
			panic(err)
		}

		tables = append(tables, table)
		table.PrettyPrint()

		err = p.CreateAndPopulateTable(table)
		if err != nil {
			panic(err)
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
				columnsWithAlias = append(columnsWithAlias, fmt.Sprintf("T%d.%s %s", i, c.Name, c.Name))
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
		viewSqlBuilder = viewSqlBuilder.JoinWithOption(sb.FullOuterJoin, table.Name+" T"+strconv.Itoa(i), onExpr...)
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

func (p *Platon) CreateAndPopulateTable(table Table) error {
	ctx := context.Background()
	dropSql := "DROP TABLE IF EXISTS " + table.Name
	fmt.Println("Executing SQL: " + dropSql)
	err := p.Database.Connection.Exec(ctx, dropSql)
	if err != nil {
		return fmt.Errorf("failed to drop cube table: %w", err)
	}

	cols := table.GetColumns()
	columns := ""
	for _, c := range cols {
		columns = columns + c.Name + " " + c.DataType + ","
	}
	columns = columns[:len(columns)-1]
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s) PRIMARY KEY(Time)", table.Name, columns)
	fmt.Println(sql)

	err = p.Database.Connection.Exec(ctx, sql)
	if err != nil {
		return fmt.Errorf("failed to create cube table: %w", err)
	}

	batch, err := p.Database.Connection.PrepareBatch(ctx, "INSERT INTO "+table.Name)
	if err != nil {
		return err
	}
	for i := 0; i < len(table.Rows); i++ {
		//fmt.Printf("Adding %v", table.Rows[i].GetOrderedValues(cols)...)
		err = batch.Append(table.Rows[i].GetOrderedValues(cols)...)
		if err != nil {
			return fmt.Errorf("failed to add row to batch: %w", err)
		}
	}
	err = batch.Send()
	if err != nil {
		return fmt.Errorf("failed to execute batch batch: %w", err)
	}
	return nil
}

var promClient api.Client
var promClientInitialized bool = false

func GetPromClient() (api.Client, error) {
	if promClientInitialized {
		return promClient, nil
	}
	address := flag.String("address", "localhost", "Prometheus address")
	port := flag.String("port", "9090", "Prometheus port")
	isSSL := flag.Bool("ssl", false, "Enable transport security")

	url := ConstructURL(*address, *port, *isSSL)
	var err error
	promClient, err = api.NewClient(api.Config{
		Address: url,
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
		samples, err := p.GetSamples(metricName)
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

func GenerateCube(cubeName string, metricNames []string) Cubes {
	cubes := Cubes{}
	p := NewPlaton()
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
		for _, label := range metric.Dimensions {
			if !slices.Contains(commonLabels, label) {
				commonLabels = slices.Delete(commonLabels, i, i)
			}
		}
	}
	cube.JoinedLabels = commonLabels
	cubes.Cubes = []Cube{cube}

	return cubes
}

func PrintDimensions() {
	p := NewPlaton()
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

func PrintMetrics(dimensionFilter []string) {
	p := NewPlaton()

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

func (p *Platon) GetSamples(metric string) (model.Value, error) {
	v1api := v1.NewAPI(p.Client)

	result, warnings, err := v1api.QueryRange(context.TODO(), metric, v1.Range{Start: p.StartTime, End: p.EndTime, Step: 1 * time.Minute}, v1.WithTimeout(5*time.Second))
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

// ConstructURL builds a URL and returns it as string
func ConstructURL(address string, port string, ssl bool) string {
	var url string
	if ssl {
		url = "https://" + address + ":" + port
	}
	if !ssl {
		url = "http://" + address + ":" + port
	}
	return url
}

func MetricsToTable(metricName string, queryResults []model.Value, cube Cube) (Table, error) {
	table := Table{
		Name:       metricName,
		Dimensions: []string{},
		Rows:       []*Row{},
	}

	for i := range queryResults {
		table.addQueryResult(cube.Queries[i], queryResults[i], cube)
	}
	fmt.Printf("Rows added to internal table: %d\n", len(table.Rows))
	return table, nil
}
