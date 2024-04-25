package platon

import (
	"os"
	"slices"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/prometheus/common/model"
)

type Table struct {
	Name       string
	Dimensions []string
	Metrics    []string
	Rows       []*Row
}

type Row struct {
	Dimensions map[string]string
	Metrics    map[string]float64
	Time       time.Time
}

type Column struct {
	Name       string
	DataType   string
	ColumnType string
}

func (t Table) GetColumns() []Column {

	cols := []Column{}

	cols = append(cols, Column{"Time", "DateTime", "Time"})
	for _, dimension := range t.Dimensions {
		cols = append(cols, Column{dimension, "String", "Dimension"})
	}
	for _, metric := range t.Metrics {
		cols = append(cols, Column{metric, "Float64", "Metric"})
	}
	return cols
}

func (r Row) GetOrderedValues(order []Column) []interface{} {
	values := []interface{}{}
	for _, col := range order {
		switch col.ColumnType {
		case "Dimension":
			values = append(values, r.Dimensions[col.Name])
		case "Metric":
			values = append(values, r.Metrics[col.Name])
		case "Time":
			values = append(values, r.Time)
		}

	}
	return values
}

func (t *Table) addQueryResult(query Query, queryResult model.Value, cube Cube) {
	matrix := queryResult.(model.Matrix)
	for _, sampleStream := range matrix {
		for _, value := range sampleStream.Values {
			timestamp := int64(value.Timestamp / 1000)
			row := NewRow(time.Unix(timestamp, 0))
			valueName := t.GetMetric(query.Value)
			row.Metrics[valueName] = float64(value.Value)

			for label, value := range sampleStream.Metric {
				if string(label) == "__name__" {
					continue
				}
				dimension := t.GetDimension(string(label))
				row.Dimensions[dimension] = string(value)
			}
			t.InsertRow(row)
		}
	}
}

func (t *Table) InsertRow(rowInput *Row) {
findMatchingRow:
	for i := range t.Rows {
		row := t.Rows[i]
		if row.Time != rowInput.Time {
			continue findMatchingRow
		}
		for dimension, value := range row.Dimensions {
			inputValue, ok := rowInput.Dimensions[dimension]
			if !ok || inputValue != value {
				continue findMatchingRow
			}
		}
		for inputDimension, value := range rowInput.Dimensions {
			row.Dimensions[inputDimension] = value
		}
		for metric, value := range rowInput.Metrics {
			row.Metrics[metric] = value
		}
		return
	}
	t.Rows = append(t.Rows, rowInput)
}

func (t *Table) PrettyPrint() {
	tab := table.NewWriter()
	tab.SetOutputMirror(os.Stdout)
	columns := t.GetColumns()
	header := table.Row{}
	types := table.Row{}
	columnTypes := table.Row{}
	for _, col := range columns {
		header = append(header, col.Name)
		types = append(types, col.DataType)
		columnTypes = append(columnTypes, col.ColumnType)
	}
	tab.AppendHeader(columnTypes)
	tab.AppendHeader(header)
	tab.AppendHeader(types)
	for _, row := range t.Rows {
		tab.AppendRows([]table.Row{
			row.GetOrderedValues(columns),
		})
	}
	tab.Render()
}

func NewRow(time time.Time) *Row {
	row := Row{
		Time:       time,
		Dimensions: map[string]string{},
		Metrics:    map[string]float64{},
	}
	return &row
}

func (t *Table) GetDimension(dimension string) string {
	if !slices.Contains(t.Dimensions, dimension) {
		t.Dimensions = append(t.Dimensions, dimension)
	}
	return dimension
}

func (t *Table) GetMetric(metric string) string {
	if !slices.Contains(t.Metrics, metric) {
		t.Metrics = append(t.Metrics, metric)
	}
	return metric
}
