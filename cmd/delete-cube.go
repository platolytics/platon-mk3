/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"

	"github.com/platolytics/platon-mk3/pkg/db/clickhouse"
	"github.com/platolytics/platon-mk3/pkg/platon"
	"github.com/spf13/cobra"
)

// cubeCmd represents the cube command
var deleteCubeCmd = &cobra.Command{
	Use:   "cube",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
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
		err = platon.DeleteCubes(clickhouse, cubes)
		if err != nil {
			panic(err)
		}
		defer clickhouse.Connection.Close()
	},
}

func init() {
	deleteCmd.AddCommand(deleteCubeCmd)
	deleteCubeCmd.Flags().StringP(cubesArg, "c", "", "File specifying cubes to build and sync")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// cubeCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// cubeCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
