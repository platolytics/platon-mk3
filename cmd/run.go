package cmd

import (
	"fmt"
	"os"

	"github.com/platolytics/platon-mk3/pkg/db/clickhouse"
	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "A brief description of your command",
	Long: `Run platon.

Specify a cubes yaml file for cubes to sync, e.g.

---
cubes:
- ttl: 1h
  scrape-interval: 1m
  promql:
  - metric1
  - metric2{pod=abc}
  name: apiserver-resource-usage
  description: API Server resource usage Analysis
`,
	Run: RunRun,
}

const (
	cubesArg string = "cubes"
)

func RunRun(cmd *cobra.Command, args []string) {
	cubeFile, _ := cmd.Flags().GetString(cubesArg)
	if cubeFile == "" {
		fmt.Printf("Please specify cubes YAML file using --%s.\n", cubesArg)
		return
	}
	cubes, err := parseCubesFile(cubeFile)
	if err != nil {
		panic(err)
	}
	clickhouse, err := clickhouse.Connect()
	if err != nil {
		panic(err)
	}
	defer clickhouse.Connection.Close()
	prometheusUrl, _ := cmd.Flags().GetString(PrometheusArg)
	platon.WatchCubes(clickhouse, cubes, prometheusUrl)
}

func parseCubesFile(cubeFile string) (cubes platon.Cubes, err error) {
	cubes.Cubes = []platon.Cube{}
	yamlFile, err := os.ReadFile(cubeFile)
	if err != nil {
		err = fmt.Errorf("can't read cube file: %w", err)
		return
	}
	err = yaml.Unmarshal(yamlFile, &cubes)
	if err != nil {
		err = fmt.Errorf("can't parse cube file: %w", err)
		return
	}
	return
}

func init() {
	rootCmd.AddCommand(runCmd)
	runCmd.Flags().StringP(cubesArg, "c", "", "File specifying cubes to build and sync")
}
