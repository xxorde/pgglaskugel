// Copyright © 2016 Alexander Sosna <alexander@xxor.de>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package cmd

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pierrec/lz4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// elefant logo
	elefant = `
      __
.-====O|\_.
  /\ /\
	`
	lz4BlockMaxSizeDefault = 4 << 20
)

var (
	// Vars for configuration
	cfgFile    string
	archiveDir string
	pgDataDir  string

	// Minimal and maximal PostgreSQL version (numeric)
	pgMinVersion           = 90500
	pgMaxVersion           = 90699
	supportedMajorVersions = [...]string{"9.5", "9.6"}

	// Maximum PID
	maxPID = 32768

	// Default number of parallel jobs
	defaultJobs = int((runtime.NumCPU() + 2) / 3)
	//  1 core =>  1 jobs
	//  2 core =>  1 jobs
	//  3 core =>  1 jobs
	//  4 core =>  2 jobs
	//  8 core =>  3 jobs
	// 16 core =>  6 jobs
	// 32 core => 11 jobs

	// Store time of programm start
	startTime time.Time

	// Set lz4 parameter
	lz4Header = lz4.Header{
		BlockDependency: false,
		BlockChecksum:   false,
		BlockMaxSize:    lz4BlockMaxSizeDefault,
		NoChecksum:      false,
		HighCompression: false,
	}

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   "pgSOSBackup",
		Short: "A tool to backup PostgreSQL databases",
		Long:  `A tool that helps you to manage your PostgreSQL backups and strategies.` + elefant,
	}
)

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := RootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

func init() {
	startTime = time.Now()
	cobra.OnInitialize(initConfig)
	// Set the default values for the globally used flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file")
	RootCmd.PersistentFlags().StringP("pgdata", "D", "$PGDATA", "Base directory of your PostgreSQL instance aka. pg_data")
	RootCmd.PersistentFlags().String("archivedir", "/var/lib/postgresql/backup/pgSOSBackup", "Dir where the backups go")
	RootCmd.PersistentFlags().Bool("debug", false, "Enable debug mode, to increase verbosity")
	RootCmd.PersistentFlags().Bool("json", false, "Generate output as JSON")
	RootCmd.PersistentFlags().String("connection", "user=postgres dbname=postgres", "Connection string to connect to the database")
	RootCmd.PersistentFlags().IntP("jobs", "j", defaultJobs, "The number of jobs to run parallel")

	// Bind flags to viper
	// Try to find better suiting values over the viper configuration files
	viper.BindPFlag("pgdata", RootCmd.PersistentFlags().Lookup("pgdata"))
	viper.BindPFlag("archivedir", RootCmd.PersistentFlags().Lookup("archivedir"))
	viper.BindPFlag("debug", RootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("json", RootCmd.PersistentFlags().Lookup("json"))
	viper.BindPFlag("connection", RootCmd.PersistentFlags().Lookup("connection"))
	viper.BindPFlag("jobs", RootCmd.PersistentFlags().Lookup("jobs"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

	// Set the priority / chain where to look for configuration files
	viper.SetConfigName("config")           // name of config file (without extension)
	viper.AddConfigPath("/etc/pgsosbackup") // adding /etc/pgSOSBackup as first search path
	viper.AddConfigPath("$HOME/.config/pgsosbackup")
	viper.AddConfigPath("$HOME/.pgsosbackup")
	viper.AutomaticEnv() // read in environment variables that match

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Info("Using config file: ", viper.ConfigFileUsed())
	}

	// Set log format to json if set
	if viper.GetBool("json") == true {
		log.SetFormatter(&log.JSONFormatter{})
	}

	// Set loglevel to debug
	if viper.GetBool("debug") == true {
		log.SetLevel(log.DebugLevel)
		log.Debug("Running with debug mode")
	}

	// Sett parallelism
	runtime.GOMAXPROCS(viper.GetInt("jobs"))

	// Set archiveDir var
	archiveDir = viper.GetString("archivedir")
	log.Debug("archiveDir: ", archiveDir)

	// Show pg_data
	pgDataDir = viper.GetString("pgdata")
	log.Debug("pgdata: ", pgDataDir)
}

// Global needed functions

// testTools test if all tools in tools are installed by trying to run them
func testTools(tools []string) (err error) {
	for _, tool := range tools {
		cmd := exec.Command(tool, "--version")
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err != nil {
			log.Debug("Output of tool: ", tool, " is: ", out.String())
			log.Warning("It seems that tool: ", tool, " is not working correctly: ", err)
		}
		log.Debug("Tool ", tool, " seems to be functional")
	}
	return err
}
