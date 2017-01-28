// Copyright © 2017 Alexander Sosna <alexander@xxor.de>
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
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	minio "github.com/minio/minio-go"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	util "github.com/xxorde/pgglaskugel/util"

	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	myName    = "pgglaskugel"
	myVersion = "0.1"

	// Logo
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
)

var (
	// own executable path
	myExecutable string

	// Vars for configuration
	cfgFile    string
	archiveDir string

	// Minimal and maximal PostgreSQL version (numeric)
	pgMinVersion           = 90500
	pgMaxVersion           = 90699
	supportedMajorVersions = [...]string{"9.5", "9.6"}

	// sub folders
	subDirBasebackup = "/basebackup/"
	subDirWal        = "/wal/"

	// commands
	cmdTar        = "tar"
	cmdBasebackup = "pg_basebackup"
	cmdZstd       = "/usr/bin/zstd"
	cmdZstdcat    = "/usr/bin/zstdcat"

	baseBackupTools = []string{
		cmdTar,
		cmdBasebackup,
		cmdZstd,
		cmdZstdcat,
	}

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

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   myName,
		Short: "A tool to backup PostgreSQL databases",
		Long:  `A tool that helps you to manage your PostgreSQL backups.` + logo,
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
	// Measure time from here
	startTime = time.Now()

	myExecutable, _ = osext.Executable()

	cobra.OnInitialize(initConfig)
	// Set the default values for the globally used flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file")
	RootCmd.PersistentFlags().StringP("pgdata", "D", "$PGDATA", "Base directory of your PostgreSQL instance aka. pg_data")
	RootCmd.PersistentFlags().Bool("pgdata-auto", true, "Try to find pgdata if not set correctly (via SQL)")
	RootCmd.PersistentFlags().String("archivedir", "/var/lib/postgresql/backup/pgglaskugel", "Dir where the backups go")
	RootCmd.PersistentFlags().Bool("debug", false, "Enable debug mode, to increase verbosity")
	RootCmd.PersistentFlags().Bool("json", false, "Generate output as JSON")
	RootCmd.PersistentFlags().String("connection", "host=/var/run/postgresql user=postgres dbname=postgres", "Connection string to connect to the database")
	RootCmd.PersistentFlags().IntP("jobs", "j", defaultJobs, "The number of jobs to run parallel, default depends on cores ")
	RootCmd.PersistentFlags().String("backup_to", "file", "Backup destination (file|s3)")
	RootCmd.PersistentFlags().String("archive_to", "file", "WAL destination (file|s3)")
	RootCmd.PersistentFlags().String("s3_endpoint", "127.0.0.1:9000", "S3 endpoint")
	RootCmd.PersistentFlags().String("s3_bucket_backup", "pgglaskugel-basebackup", "Bucket name for base backups")
	RootCmd.PersistentFlags().String("s3_bucket_wal", "pgglaskugel-wal", "Bucket name for WAL files")
	RootCmd.PersistentFlags().String("s3_access_key", "TUMO1VCSJF7R2LC39A24", "access_key")
	RootCmd.PersistentFlags().String("s3_secret_key", "yOzp7WVWOs9mFeqATXmcQQ5crv4IQtQUv1ArzdYC", "secret_key")
	RootCmd.PersistentFlags().String("s3_location", "us-east-1", "S3 datacenter location")
	RootCmd.PersistentFlags().Bool("s3_ssl", true, "If SSL (TLS) should be used for S3")

	// Bind flags to viper
	// Try to find better suiting values over the viper configuration files
	viper.BindPFlag("pgdata", RootCmd.PersistentFlags().Lookup("pgdata"))
	viper.BindPFlag("pgdata-auto", RootCmd.PersistentFlags().Lookup("pgdata-auto"))
	viper.BindPFlag("archivedir", RootCmd.PersistentFlags().Lookup("archivedir"))
	viper.BindPFlag("debug", RootCmd.PersistentFlags().Lookup("debug"))
	viper.BindPFlag("json", RootCmd.PersistentFlags().Lookup("json"))
	viper.BindPFlag("connection", RootCmd.PersistentFlags().Lookup("connection"))
	viper.BindPFlag("jobs", RootCmd.PersistentFlags().Lookup("jobs"))
	viper.BindPFlag("backup_to", RootCmd.PersistentFlags().Lookup("backup_to"))
	viper.BindPFlag("archive_to", RootCmd.PersistentFlags().Lookup("archive_to"))
	viper.BindPFlag("s3_endpoint", RootCmd.PersistentFlags().Lookup("s3_endpoint"))
	viper.BindPFlag("s3_bucket_backup", RootCmd.PersistentFlags().Lookup("s3_bucket_backup"))
	viper.BindPFlag("s3_bucket_wal", RootCmd.PersistentFlags().Lookup("s3_bucket_wal"))
	viper.BindPFlag("s3_access_key", RootCmd.PersistentFlags().Lookup("s3_access_key"))
	viper.BindPFlag("s3_secret_key", RootCmd.PersistentFlags().Lookup("s3_secret_key"))
	viper.BindPFlag("s3_location", RootCmd.PersistentFlags().Lookup("s3_location"))
	viper.BindPFlag("s3_ssl", RootCmd.PersistentFlags().Lookup("s3_ssl"))
}

// initConfig reads in config file and ENV variables if set.
func initConfig() {
	// Set the priority / chain where to look for configuration files
	viper.SetConfigName("config")           // name of config file (without extension)
	viper.AddConfigPath("/etc/pgglaskugel") // adding /etc/pgglaskugel as first search path
	viper.AddConfigPath("$HOME/.config/pgglaskugel")
	viper.AddConfigPath("$HOME/.pgglaskugel")
	viper.AddConfigPath("$PWD/.pgglaskugel")
	viper.AddConfigPath("$PGDATA/.pgglaskugel")
	viper.AutomaticEnv() // read in environment variables that match

	if cfgFile != "" { // enable ability to specify config file via flag
		viper.SetConfigFile(cfgFile)
	}

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

	// Check if needed tools are available
	err := testTools(baseBackupTools)
	ec.Check(err)
}

// Global needed functions

func printDone() {
	elapsed := time.Since(startTime)
	log.Info("Done in ", elapsed)
}

// testTools test if all tools in tools are installed by trying to run them
func testTools(tools []string) (err error) {
	for _, tool := range tools {
		cmd := exec.Command(tool, "--version")
		var out bytes.Buffer
		cmd.Stdout = &out
		err := cmd.Run()
		if err == nil {
			log.Debug("Tool ", tool, " seems to be functional")
			return nil
		}
		log.Debug("Output of tool: ", tool, " is: ", out.String())
		log.Warning("It seems that tool: ", tool, " is not working correctly: ", err)
	}
	return err
}

// validatePgData validates a given pgData path
func validatePgData(pgData string) (err error) {
	_, err = getMajorVersionFromPgData(pgData)
	if err != nil {
		err = errors.New("Can not validate pg_data: " + pgData + " error:" + err.Error())
	}
	return err
}

// reloadConfiguration reloads the PostgreSQL configuration
func reloadConfiguration(db *sql.DB) (err error) {
	query := "SELECT pg_reload_conf();"
	_, err = db.Query(query)
	ec.Check(err)
	return err
}

// getPgSetting gets the value for a given setting in the current PostgreSQL configuration
func getPgSetting(db *sql.DB, setting string) (value string, err error) {
	query := "SELECT setting FROM pg_settings WHERE name = $1;"
	row := db.QueryRow(query, setting)
	ec.Check(err)
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

// Get pg_data from viper or try to find it (via SQL)
func getPgData(db *sql.DB) (pgData string, err error) {
	pgData = viper.GetString("pgdata")
	err = validatePgData(pgData)
	if viper.GetBool("pgdata-auto") == false {
		log.Debug("pgdata-auto is false, using config value for pgdata, ", pgData)
		return pgData, err
	}

	// If pg_data is not valid try to get it from PostgreSQL
	if err != nil {
		log.Debug("pgdata is not valid, try to it via SQL")
		pgData, err = getPgSetting(db, "data_directory")
		if err != nil {
			log.Warn("pg_data was not set correctly, can not get it via SQL: ", err)
		} else {
			// Try to validate pg_data from SQL
			err = validatePgData(pgData)
			if err != nil {
				log.Warn("Can not validate pg_data: ", pgData)
			} else {
				log.Info("Got pg_data via SQL: ", pgData)
			}
		}
	}

	return pgData, err
}

// getMajorVersionFromPgData looks in pgData and returns the major version of PostgreSQL
func getMajorVersionFromPgData(pgData string) (pgMajorVersion string, err error) {
	versionFile := pgData + "/PG_VERSION"

	dat, err := ioutil.ReadFile(versionFile)
	if err != nil {
		log.Debug("Can not open PG_VERSION file ", versionFile)
		return "", err
	}

	pgMajorVersion = strings.TrimSpace(string(dat))

	if isMajorVersionSupported(pgMajorVersion) != true {
		err = errors.New("The PostgreSQL major version: " + pgMajorVersion + " is not in the supported list")
	}

	return pgMajorVersion, err
}

// checkPgVersion checks if PostgreSQL Version is supported via SQL
func checkPgVersion(db *sql.DB) (pgVersion pgVersion, err error) {
	pgVersion.string, err = getPgSetting(db, "server_version")
	if err != nil {
		log.Fatal("Can not get server_version!")
		return pgVersion, err
	}

	numString, err := getPgSetting(db, "server_version_num")
	if err != nil {
		log.Fatal("Can not get server_version_num!")
		return pgVersion, err
	}
	pgVersion.num, err = strconv.Atoi(numString)
	if err != nil {
		log.Fatal("Can not parse server_version_num!")
		return pgVersion, err
	}

	log.Debug("pgVersion ", pgVersion)

	if isPgVersionSupported(pgVersion.num) != true {
		log.Fatal("Please check for a compatible version.")
	}

	return pgVersion, err
}

func checkNeededParameter(parameter ...string) (err error) {
	errCount := 0
	for _, p := range parameter {
		if viper.GetString(p) <= "" {
			errCount++
			log.Warn(p, " ist not set")
		}
	}
	if errCount > 0 {
		return errors.New("No all parameters are set")
	}
	return nil
}

func getS3Connection() (minioClient minio.Client) {
	endpoint := viper.GetString("s3_endpoint")
	accessKeyID := viper.GetString("s3_access_key")
	secretAccessKey := viper.GetString("s3_secret_key")
	ssl := viper.GetBool("s3_ssl")

	// Initialize minio client object.
	tmp, err := minio.New(endpoint, accessKeyID, secretAccessKey, ssl)
	if err != nil {
		log.Fatal(err)
	}

	tmp.SetAppInfo(myName, myVersion)
	log.Debugf("%v", minioClient)

	return *tmp
}

func getMyBackups() (backups util.Backups) {
	backupDir := archiveDir + "/basebackup"

	log.Debug("Get backups from folder: ", backupDir)
	backups.GetBackupsInDir(backupDir)
	backups.WalDir = viper.GetString("archivedir") + "/wal"

	if viper.GetString("backup_to") == "s3" {
		log.Debug("Get backups from S3")

		// Initialize minio client object.
		backups.MinioClient = getS3Connection()
		backups.GetBackupsInBucket(viper.GetString("s3_bucket_backup"))
		backups.WalBucket = viper.GetString("s3_bucket_wal")
	}
	return backups
}
