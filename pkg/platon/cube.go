package platon

import (
	"time"
)

type Cubes struct {
	Cubes []Cube `yaml:"cubes"`
}

type Cube struct {
	Name           string        `yaml:"name"`
	Description    string        `yaml:"description"`
	Ttl            time.Duration `yaml:"ttl"`
	ScrapeInterval time.Duration `yaml:"scrape-interval"`
	Queries        []Query       `yaml:"queries"`
	JoinedLabels   []string      `yaml:"joined-labels"`
	LastUpdate     time.Time
	//labels         []string
}

type Query struct {
	Name        string `yaml:"name"`
	PromQL      string `yaml:"promql"`
	Value       string `yaml:"value"`
	Aggregation string `yaml:"aggregation"`
}

func (c *Cube) GetMetricColumns() []string {
	cols := []string{}
	for _, q := range c.Queries {
		cols = append(cols, q.Name)
	}
	return cols
}

func (c *Cube) GetAggregation(query string) string {
	for _, q := range c.Queries {
		if q.Name == query {
			return q.Aggregation
		}
	}
	return "SUM"
}
