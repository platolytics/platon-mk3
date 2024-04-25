package platon

import "time"

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
	//lastUpdate     time.Time
	//labels         []string
}

type Query struct {
	Name   string `yaml:"name"`
	PromQL string `yaml:"promql"`
	Value  string `yaml:"value"`
}
