/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v2"
)

const (
	metricsArg string = "dimensionfilter"
	nameArg    string = "name"
)

// cubeCmd represents the cube command
var cubeCmd = &cobra.Command{
	Use:   "cube",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		name, _ := cmd.Flags().GetString(nameArg)
		metrics, _ := cmd.Flags().GetStringArray(dimensionsArg)
		cube := platon.GenerateCube(name, metrics)
		yamlBytes, err := yaml.Marshal(cube)
		if err != nil {
			panic(err)
		}
		fmt.Println(string(yamlBytes))
	},
}

func init() {
	generateCmd.AddCommand(cubeCmd)
	cubeCmd.Flags().StringP(nameArg, "n", "MyCube", "Cube name")
	cubeCmd.Flags().StringArrayP(metricsArg, "m", []string{}, "Metrics to include in cube")

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// cubeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// cubeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
