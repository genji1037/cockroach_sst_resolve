package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
)

var rootCmd = &cobra.Command{
	Use:   "cockroach_sst_resolve",
	Short: "cockroach_sst_resolve is used to translate cockroach sst file to sql",
	Long:  "cockroach_sst_resolve is used to translate cockroach sst file to sql",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("welcome to cockroach_sst_resolve")
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
