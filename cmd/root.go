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
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/pierrec/lz4"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// elefant logo
	logo = `
     __________
    /          \
   /   ______   \
  /   /     0\   \
 /   /        \   \
 \   \        /   /
  \   \______/   /
   \  /______\  /
    \__________/
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
		Long:  `A tool that helps you to manage your PostgreSQL backups and strategies.` + logo,
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
	RootCmd.PersistentFlags().Bool("pgdata-auto", true, "Try to find pgdata if not set correctly (via SQL)")
	RootCmd.PersistentFlags().String("archivedir", "/var/lib/postgresql/backup/pgSOSBackup", "Dir where the backups go")
	RootCmd.PersistentFlags().Bool("debug", false, "Enable debug mode, to increase verbosity")
	RootCmd.PersistentFlags().Bool("json", false, "Generate output as JSON")
	RootCmd.PersistentFlags().String("connection", "user=postgres dbname=postgres", "Connection string to connect to the database")
	RootCmd.PersistentFlags().IntP("jobs", "j", defaultJobs, "The number of jobs to run parallel, default depends on cores ")

	// Bind flags to viper
	// Try to find better suiting values over the viper configuration files
	viper.BindPFlag("pgdata", RootCmd.PersistentFlags().Lookup("pgdata"))
	viper.BindPFlag("pgdata-auto", RootCmd.PersistentFlags().Lookup("pgdata"))
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

// validatePgData validates a given pgData path
func validatePgData(pgData string) (err error) {
	_, err = getMajorVersionFromPgData(pgData)
	if err != nil {
		log.Debug("Can not validate pg_data: ", pgData, " error:", err)
	}
	return err
}

func check(err error) error {
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// reloadConfiguration reloads the PostgreSQL configuration
func reloadConfiguration(db *sql.DB) (err error) {
	query := "SELECT pg_reload_conf();"
	_, err = db.Query(query)
	check(err)
	return err
}

// getPgSetting gets the value for a given setting in the current PostgreSQL configuration
func getPgSetting(db *sql.DB, setting string) (value string, err error) {
	query := "SELECT setting FROM pg_settings WHERE name = $1;"
	row := db.QueryRow(query, setting)
	check(err)
	err = row.Scan(&value)
	if err != nil {
		log.Fatal("Can't get PostgreSQL setting: ", setting, " err:", err)
		return "", err
	}
	log.Debug("Got ", value, " for ", setting, " in pg_settings")
	return value, nil
}

// setPgSetting sets a value to a setting
func setPgSetting(db *sql.DB, setting string, value string) (err error) {
	// Bad style and risk for injection!!! But no better option ... open for suggestions!
	query := "ALTER SYSTEM SET " + setting + " = '" + value + "';"
	_, err = db.Query(query)
	if err != nil {
		log.Fatal("Can't set PostgreSQL setting: ", setting, " to: ", value, " Error: ", err)
		return err
	}
	log.Info("Set PostgreSQL setting: ", setting, " to: ", value)
	return nil
}

// getPostmasterPID returns the PID of the postmaster process found in the pid file
func getPostmasterPID(pgData string) (postmasterPID int, err error) {
	pidFile := pgData + "/postmaster.pid"
	postmasterPID = -1
	file, err := os.Open(pidFile)
	if err != nil {
		log.Error("Can not open PID file ", pidFile)
		return postmasterPID, err
	}

	scanner := bufio.NewScanner(file)

	// Read first line
	scanner.Scan()
	line := scanner.Text()

	postmasterPID, err = strconv.Atoi(line)
	if err != nil {
		log.Error("Can not parse postmaster PID: ", string(line), " from: ", pidFile)
	}

	if postmasterPID < 1 {
		log.Error("PID found in ", pidFile, " is to low: ", postmasterPID)
	}

	if postmasterPID > maxPID {
		log.Error("PID found in ", pidFile, " is to high: ", postmasterPID)
	}

	return postmasterPID, err
}

// If pg_data is not valid try to get it from PostgreSQL
func getPgData(db *sql.DB) (pgDataDir string, err error) {
	pgDataDir, err = getPgSetting(db, "data_directory")
	if err != nil {
		log.Warn("pg_data was not set correctly, can not get it via SQL: ", err)
	} else {
		// Try to validate pg_data from SQL
		err = validatePgData(pgDataDir)
		if err != nil {
			log.Warn("Can not validate pg_data: ", pgDataDir)
		} else {
			log.Info("Got pg_data via SQL: ", pgDataDir)
		}
	}
	return pgDataDir, err
}
