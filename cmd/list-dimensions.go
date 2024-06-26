/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
)

const (
	metricsFilterArg string = "metricfilter"
)

// dimensionsCmd represents the dimensions command
var dimensionsCmd = &cobra.Command{
	Use:   "dimensions",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		prometheusUrl, _ := cmd.Flags().GetString(PrometheusArg)
		metricsFilter, _ := cmd.Flags().GetStringArray(metricsFilterArg)
		platon.PrintDimensions(metricsFilter, prometheusUrl)
	},
}

func init() {
	listCmd.AddCommand(dimensionsCmd)
	dimensionsCmd.Flags().StringArrayP(metricsFilterArg, "m", []string{}, "metrics to query")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// dimensionsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// dimensionsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
