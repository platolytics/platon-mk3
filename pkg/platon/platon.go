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

	p.watchCubes()
}

func DeleteCubes(clickhouse clickhouse.Clickhouse, cubes Cubes) error {
	p := NewPlaton("")
	p.Cubes = cubes
	p.Database = clickhouse

	for _, cube := range p.Cubes.Cubes {
		sql := fmt.Sprintf("DROP VIEW IF EXISTS %s", cube.Name)
		fmt.Println(sql)

		err := p.Database.Connection.Exec(p.ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to create cube table: %w", err)
		}
		for _, query := range cube.Queries {
			sql := fmt.Sprintf("DROP TABLE IF EXISTS %s", query.Name)
			fmt.Println(sql)

			err := p.Database.Connection.Exec(p.ctx, sql)
			if err != nil {
				return fmt.Errorf("failed to drop metrics table: %w", err)
			}
		}
	}
	return nil
}

func ValueHelp(metric, dimension, prometheusUrl string) error {
	p := NewPlaton(prometheusUrl)

	values, err := p.queryValues(metric, dimension)
	if err != nil {
		return fmt.Errorf("failed to query metric %s: %w", metric, err)
	}

	fmt.Printf("%d values found for dimension %s in metric %s in the past hour:\n", len(values), dimension, metric)
	for _, value := range values {
		fmt.Println(value)
	}

	return nil
}

func (p *Platon) queryValues(metric, dimension string) ([]string, error) {
	values := []string{}
	samples, err := p.GetSamples(metric, time.Now().Add(-1*time.Hour), time.Now())
	if err != nil {
		return values, fmt.Errorf("failed to query prometheus for metric %s: %w", metric, err)
	}
	matrix := samples.(model.Matrix)
	for _, sampleStream := range matrix {
		for label, value := range sampleStream.Metric {
			if string(label) != dimension || slices.Contains(values, string(value)) {
				continue
			}
			values = append(values, string(value))
		}
	}
	return values, nil

}

func (p *Platon) watchCubes() {
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
	fullTable, err := p.generateFullTable(cube, tables)
	if err != nil {
		panic(err)
	}

	err = p.EnsureTable(fullTable)
	if err != nil {
		panic(err)
	}

	err = p.InsertData(fullTable)
	if err != nil {
		panic(fmt.Errorf("failed to add data to table %s: %v", fullTable.Name, err))
	}

	//err := p.createView(cube, tables)
	//if err != nil {
	//	panic(err)
	//}
}

func (p *Platon) generateFullTable(cube Cube, tables []Table) (Table, error) {
	left := tables[0]
	for i, t := range tables {
		if i == 0 {
			continue
		}

		var err error
		left, err = left.platonLeftJoin(cube, t)
		if err != nil {
			return Table{}, fmt.Errorf("failed to platon left join tables %s and %s: %w", left.Name, t.Name, err)
		}
	}

	left.Name = cube.Name

	return left, nil
}

func (left Table) platonLeftJoin(cube Cube, right Table) (Table, error) {
	joinedTable := Table{
		Dimensions: []string{},
		Rows:       []*Row{},
	}

	// Set up dimensions & metrics
	joinedTable.Dimensions = append(joinedTable.Dimensions, left.Dimensions...)
	joinedTable.Metrics = append(joinedTable.Metrics, left.Metrics...)
	for _, d := range right.Dimensions {
		col := d
		if !slices.Contains(cube.JoinedLabels, d) && d != "Time" {
			col = fmt.Sprintf("%s.%s", right.Name, d)
		}
		if !slices.Contains(joinedTable.Dimensions, col) {
			joinedTable.Dimensions = append(joinedTable.Dimensions, col)
		}
	}
	joinedTable.Metrics = append(joinedTable.Metrics, right.Metrics...)

	// Insert data from left table, just insert
	for _, r := range left.Rows {
		joinedTable.InsertRow(r)
	}

	// Right table - for each row:
	// * Count matches (all joined dimensions match) in left table.
	// ** If 0 matches, create a new row, setting all non-existent dimensions in the right table to NULL
	// ** If one match, count matches in right table.
	// *** If one match, insert values
	// *** If more than one match, create a new row, setting all non-existent dimensions in the right table to '*'
	// *** Update matching row in joined table row and set all dimensions of the right table to '*'
	// ** If more than one match, create a new row, setting all non-existent dimensions in right table to '*'

	for _, r := range right.Rows {
		leftMatches, err := left.CountMatches(append(cube.JoinedLabels, "Time"), r)
		if err != nil {
			return Table{}, fmt.Errorf("failed to count matches between tables %s and %s", left.Name, right.Name)
		}
		switch leftMatches {
		case 0:
			newRow := Row{Time: r.Time, Metrics: r.Metrics}
			for d, v := range r.Dimensions {
				newDim := d
				if !slices.Contains(cube.JoinedLabels, d) {
					newDim = fmt.Sprintf("%s.%s", right.Name, d)
				}
				newRow.Dimensions[newDim] = v
			}
			joinedTable.InsertRow(&newRow)
		case 1:
			rightMatches, err := right.CountMatches(cube.JoinedLabels, r)
			if err != nil {
				return Table{}, fmt.Errorf("failed to count matches between tables %s and %s", left.Name, right.Name)
			}
			matchingRow, err := joinedTable.GetFirstMatchingRow(cube.JoinedLabels, r)
			if err != nil {
				return Table{}, fmt.Errorf("failed to get matching row in joined table when joining %s and %s.", left.Name, right.Name)
			}
			if rightMatches == 1 {
				for d, v := range r.Dimensions {
					newDim := d
					if !slices.Contains(cube.JoinedLabels, d) {
						newDim = fmt.Sprintf("%s.%s", right.Name, d)
					}
					matchingRow.Dimensions[newDim] = v
				}
				for m, v := range r.Metrics {
					matchingRow.Metrics[m] = v
				}
				continue
			}

			newRow := Row{Time: r.Time, Metrics: r.Metrics}
			for d, v := range r.Dimensions {
				newDim := d
				if !slices.Contains(cube.JoinedLabels, d) {
					newDim = fmt.Sprintf("%s.%s", right.Name, d)
				}
				newRow.Dimensions[newDim] = v
				matchingRow.Dimensions[newDim] = "*"
			}
			for _, d := range joinedTable.Dimensions {
				_, ok := newRow.Dimensions[d]
				if !ok {
					newRow.Dimensions[d] = "*"
				}
			}
			joinedTable.InsertRow(&newRow)

		default:
			newRow := Row{Time: r.Time, Metrics: r.Metrics}
			for d, v := range r.Dimensions {
				newDim := d
				if !slices.Contains(cube.JoinedLabels, d) {
					newDim = fmt.Sprintf("%s.%s", right.Name, d)
				}
				newRow.Dimensions[newDim] = v
			}
			for _, d := range joinedTable.Dimensions {
				_, ok := newRow.Dimensions[d]
				if !ok {
					newRow.Dimensions[d] = "*"
				}
			}
			joinedTable.InsertRow(&newRow)
		}
	}

	return joinedTable, nil
}

func (p *Platon) createView(cube Cube, tables []Table) error {

	columnsWithAlias := []string{}
	for i, t := range tables {
		for _, c := range t.GetColumns() {
			if slices.Contains(cube.GetMetricColumns(), c.Name) {
				// metric column, aggregate values
				columnsWithAlias = append(columnsWithAlias, fmt.Sprintf("%s(\"T%d\".\"%s\") \"%s\"", cube.GetAggregation(c.Name), i, c.Name, c.Name))
				continue
			}
			if i == 0 && (slices.Contains(cube.JoinedLabels, c.Name) || c.Name == "Time") {
				// join key, use original value
				columnsWithAlias = append(columnsWithAlias, fmt.Sprintf("\"T%d\".\"%s\" \"%s\"", i, c.Name, c.Name))
				continue
			}
			// additional column, make array and prefix column name
			columnsWithAlias = append(columnsWithAlias, fmt.Sprintf("\"T%d\".\"%s\" \"%s.%s\"", i, c.Name, t.Name, c.Name))
		}
	}

	ctx := context.Background()

	viewSqlBuilder := sb.ClickHouse.NewSelectBuilder()
	viewSqlBuilder = viewSqlBuilder.Select(columnsWithAlias...)
	unionSqls := []string{}

	joinCols := append(cube.JoinedLabels, "Time")
	leftTableColumns := []string{}
	for _, c := range tables[0].GetColumns() {
		if slices.Contains(joinCols, c.Name) {
			leftTableColumns = append(leftTableColumns, c.Name)
			continue
		}
		leftTableColumns = append(leftTableColumns, fmt.Sprintf("MAX(\"%s\") \"%s\"", c.Name, c.Name))
	}

	for i, table := range tables {
		if i == 0 {
			viewSqlBuilder = viewSqlBuilder.From(table.Name + " T0")
			continue
		}
		onExpr := []string{}
		for _, joinCol := range joinCols {
			onExpr = append(onExpr, fmt.Sprintf("T0.%s=T%d.%s", joinCol, i, joinCol))
		}
		rightTableColumns := []string{}
		for _, c := range table.GetColumns() {
			if slices.Contains(joinCols, c.Name) {
				rightTableColumns = append(rightTableColumns, c.Name)
				continue
			}
			rightTableColumns = append(rightTableColumns, fmt.Sprintf("MAX(\"%s\") \"%s\"", c.Name, c.Name))
		}
		rightTableJoinBuilder := sb.ClickHouse.NewSelectBuilder().Select(rightTableColumns...).From(table.Name + " T" + strconv.Itoa(i))
		rightTableJoinBuilder = rightTableJoinBuilder.GroupBy(joinCols...).Having("COUNT(Time) = 1")
		rightTableSelect, _ := rightTableJoinBuilder.Build()

		leftTableJoinBuilder := sb.ClickHouse.NewSelectBuilder().Select(leftTableColumns...).From(table.Name)
		leftTableJoinBuilder = leftTableJoinBuilder.GroupBy(joinCols...).Having("COUNT(Time) != 1")
		leftTableSelect, _ := leftTableJoinBuilder.Build()

		rightTableUnionBuilder := sb.ClickHouse.NewSelectBuilder().Select(rightTableColumns...).From(table.Name+" T"+strconv.Itoa(i)).JoinWithOption("INNER ANY", "("+leftTableSelect+") T0", onExpr...).GroupBy(joinCols...)
		rightTableUnionSql, _ := rightTableUnionBuilder.Build()
		unionSqls = append(unionSqls, rightTableUnionSql)

		viewSqlBuilder = viewSqlBuilder.JoinWithOption("LEFT", "("+rightTableSelect+") T1", onExpr...).GroupBy(joinCols...)
	}
	selectSql, _ := viewSqlBuilder.Build()
	viewSql := "CREATE OR REPLACE VIEW " + cube.Name + " AS " + selectSql
	if len(unionSqls) > 0 {
		viewSql += " UNION ALL " + strings.Join(unionSqls, " UNION ALL ")
	}

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

func PrintDimensions(metricsFilter []string, prometheusUrl string) {
	p := NewPlaton(prometheusUrl)
	metrics, err := p.GetMetrics(metricsFilter...)
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
