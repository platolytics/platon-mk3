package cmd

import (
	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
)

const (
	dimensionsArg string = "dimensionfilter"
)

// metricsCmd represents the metrics command
var metricsCmd = &cobra.Command{
	Use:   "metrics",
	Short: "List all metrics available in Prometheus instance",
	Long:  ``,
	Run: func(cmd *cobra.Command, args []string) {
		dimensionFilter, _ := cmd.Flags().GetStringArray(dimensionsArg)
		prometheusUrl, _ := cmd.Flags().GetString(PrometheusArg)
		platon.PrintMetrics(dimensionFilter, prometheusUrl)
	},
}

func init() {
	listCmd.AddCommand(metricsCmd)
	metricsCmd.Flags().StringArrayP(dimensionsArg, "d", []string{}, "List only metrics with these dimensions")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// metricsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// metricsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
