/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
)

const (
	dimensionArg string = "dimension"
	metricArg    string = "metric"
)

// valuehelpCmd represents the valuehelp command
var valuehelpCmd = &cobra.Command{
	Use:   "valuehelp",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		metric, _ := cmd.Flags().GetString(metricArg)
		if metric == "" {
			fmt.Printf("Please specify metric with --%s.\n", metricArg)
			return
		}
		dimension, _ := cmd.Flags().GetString(dimensionArg)
		if dimension == "" {
			fmt.Printf("Please specify dimension with --%s.\n", dimensionArg)
			return
		}
		prometheusUrl, _ := cmd.Flags().GetString(PrometheusArg)
		platon.ValueHelp(metric, dimension, prometheusUrl)
	},
}

func init() {
	rootCmd.AddCommand(valuehelpCmd)
	valuehelpCmd.Flags().StringP(dimensionArg, "d", "", "dimension to query")
	valuehelpCmd.Flags().StringP(metricArg, "m", "", "metric to query")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// valuehelpCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// valuehelpCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
