package platon

import (
	"fmt"
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

func (t Table) GetQuotedColumnNames() []string {

	cols := []string{}

	cols = append(cols, "\"Time\"")
	for _, dimension := range t.Dimensions {
		cols = append(cols, fmt.Sprintf("\"%s\"", dimension))
	}
	for _, metric := range t.Metrics {
		cols = append(cols, fmt.Sprintf("\"%s\"", metric))
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
			val, ok := r.Metrics[col.Name]
			if ok {
				values = append(values, val)
				continue
			}
			values = append(values, nil)

		case "Time":
			values = append(values, r.Time.UTC())
		}

	}
	return values
}

func (t *Table) addQueryResult(query Query, queryResult model.Value) {
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
	t.Rows = append(t.Rows, rowInput)
}

func (t *Table) PrettyPrint(limit int) {
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
	for i, row := range t.Rows {
		if i >= limit {
			break
		}
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

func (t *Table) CountMatches(dimensions []string, otherRow *Row) (count uint64, err error) {
	for _, r := range t.Rows {
		for _, d := range dimensions {
			val, ok := r.Dimensions[d]
			if !ok {
				err = fmt.Errorf("dimension %s doesn't exist in table %s", d, t.Name)
				return
			}
			otherVal, ok := otherRow.Dimensions[d]
			if !ok {
				err = fmt.Errorf("dimension %s doesn't exist in other row", d)
				return
			}
			if val != otherVal {
				continue
			}
			if r.Time != otherRow.Time {
				continue
			}
		}
		count++
	}

	return
}

func (t *Table) GetFirstMatchingRow(dimensions []string, otherRow *Row) (*Row, error) {
	for i, r := range t.Rows {
		for _, d := range dimensions {
			val, ok := r.Dimensions[d]
			if !ok {
				return nil, fmt.Errorf("dimension %s doesn't exist in table %s", d, t.Name)
			}
			otherVal, ok := otherRow.Dimensions[d]
			if !ok {
				return nil, fmt.Errorf("dimension %s doesn't exist in other row", d)
			}
			if val != otherVal {
				continue
			}
			if r.Time != otherRow.Time {
				continue
			}
		}
		return t.Rows[i], nil
	}
	return nil, fmt.Errorf("no matching row found in table %s", t.Name)
}
