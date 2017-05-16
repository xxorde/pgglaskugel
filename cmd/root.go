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
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	ec "github.com/xxorde/pgglaskugel/errorcheck"
	util "github.com/xxorde/pgglaskugel/util"

	"github.com/kardianos/osext"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"net/http"
	// Enable server runtime profiling
	_ "net/http/pprof"

	"github.com/xxorde/pgglaskugel/storage"
)

const (
	myName = "pgglaskugel"

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
	// Version holds the current version
	Version string

	// GitHash holds the hash for the current commit
	GitHash string

	// Name of the current host
	hostname string

	// Name of the PostgreSQL cluster
	clusterName string

	// own executable path
	myExecutable string

	// Vars for configuration
	cfgFile    string
	archiveDir string
	backupDir  string
	walDir     string

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
	cmdZstd       = "zstd"
	cmdZstdcat    = "zstdcat"
	cmdGpg        = "gpg"

	baseBackupTools = []string{
		cmdTar,
		cmdBasebackup,
		cmdZstd,
		cmdZstdcat,
		cmdGpg,
	}

	// Default number of parallel jobs
	defaultJobs = runtime.NumCPU()

	// Store time of programm start
	startTime time.Time

	// PGP keys for encryption
	keyDir = "~/.pgglaskugel/"

	// RootCmd represents the base command when called without any subcommands
	RootCmd = &cobra.Command{
		Use:   myName,
		Short: "A tool to backup PostgreSQL databases",
		Long:  `A tool that helps you to manage your PostgreSQL backups.` + logo,
	}
)

// storeStream is an interface for functions that store a stream in an storage backend
type storeStream func(*io.Reader, string)

// Execute adds all child commands to the root command sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	pidfile := viper.GetString("pidpath")
	log.Debugf("pidfile is %s", pidfile)
	if err := util.WritePidFile(pidfile); err != nil {
		log.Error(err)
		os.Exit(1)
	} else {
		defer util.DeletePidFile(pidfile)
		if err := RootCmd.Execute(); err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	}
}

func init() {
	// Measure time from here
	startTime = time.Now()

	// Local error var declared here to make it easier to define the scope of other vars
	var err error

	myExecutable, err = osext.Executable()
	if err != nil {
		log.Warn(err)
	}

	hostname, err = os.Hostname()
	if err != nil {
		log.Warn(err)
	}

	cobra.OnInitialize(initConfig)
	// Set the default values for the globally used flags
	RootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "Config file")
	RootCmd.PersistentFlags().String("cluster_name", hostname, "Name of the cluster, used in backup name")
	RootCmd.PersistentFlags().StringP("pgdata", "D", "$PGDATA", "Base directory of your PostgreSQL instance aka. pg_data")
	RootCmd.PersistentFlags().Bool("pgdata-auto", true, "Try to find pgdata if not set correctly (via SQL)")
	RootCmd.PersistentFlags().String("archivedir", "/var/lib/postgresql/backup/pgglaskugel", "Dir where the backups should be stored")
	RootCmd.PersistentFlags().Bool("debug", false, "Enable debug mode to increase verbosity")
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
	RootCmd.PersistentFlags().Int("s3_protocol_version", -1, "Version of the S3 protocol version (2,4,-1=auto)")
	RootCmd.PersistentFlags().Int("s3_part_size_mb", 64, "If a part size is needed this will be used, size in MB, min: 5 MB")
	RootCmd.PersistentFlags().Bool("encrypt", false, "Enable encryption for S3 and/or file storage")
	RootCmd.PersistentFlags().StringArray("recipient", []string{"pgglaskugel"}, "The recipient for PGP encryption (key identifier)")
	RootCmd.PersistentFlags().String("path_to_tar", "/bin/tar", "Path to the tar command")
	RootCmd.PersistentFlags().String("path_to_basebackup", "/usr/bin/pg_basebackup", "Path to the basebackup command")
	RootCmd.PersistentFlags().String("path_to_zstd", "/usr/bin/zstd", "Path to the zstd command")
	RootCmd.PersistentFlags().String("path_to_zstdcat", "/usr/bin/zstdcat", "Path to the zstdcat command")
	RootCmd.PersistentFlags().String("path_to_gpg", "/usr/bin/gpg", "Path to the gpg command")
	RootCmd.PersistentFlags().Bool("no_tool_check", false, "Do not check the used tools")
	RootCmd.PersistentFlags().String("cpuprofile", "", "Write cpu profile to given filename")
	RootCmd.PersistentFlags().String("memprofile", "", "Write memory profile to given filename")
	RootCmd.PersistentFlags().Bool("http_pprof", false, "Start net/http/pprof profiler")
	RootCmd.PersistentFlags().String("pidpath", "/var/tmp/pgglaskugel/pgglaskugel.pid", "path and name for the pidfile")

	// Bind flags to viper
	// Try to find better suiting values over the viper configuration files
	viper.BindPFlag("cluster_name", RootCmd.PersistentFlags().Lookup("cluster_name"))
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
	viper.BindPFlag("s3_protocol_version", RootCmd.PersistentFlags().Lookup("s3_protocol_version"))
	viper.BindPFlag("s3_part_size_mb", RootCmd.PersistentFlags().Lookup("s3_part_size_mb"))
	viper.BindPFlag("encrypt", RootCmd.PersistentFlags().Lookup("encrypt"))
	viper.BindPFlag("recipient", RootCmd.PersistentFlags().Lookup("recipient"))
	viper.BindPFlag("path_to_tar", RootCmd.PersistentFlags().Lookup("path_to_tar"))
	viper.BindPFlag("path_to_basebackup", RootCmd.PersistentFlags().Lookup("path_to_basebackup"))
	viper.BindPFlag("path_to_zstd", RootCmd.PersistentFlags().Lookup("path_to_zstd"))
	viper.BindPFlag("path_to_zstdcat", RootCmd.PersistentFlags().Lookup("path_to_zstdcat"))
	viper.BindPFlag("path_to_gpg", RootCmd.PersistentFlags().Lookup("path_to_gpg"))
	viper.BindPFlag("no_tool_check", RootCmd.PersistentFlags().Lookup("no_tool_check"))
	viper.BindPFlag("cpuprofile", RootCmd.PersistentFlags().Lookup("cpuprofile"))
	viper.BindPFlag("memprofile", RootCmd.PersistentFlags().Lookup("memprofile"))
	viper.BindPFlag("http_pprof", RootCmd.PersistentFlags().Lookup("http_pprof"))
	viper.BindPFlag("pidpath", RootCmd.PersistentFlags().Lookup("pidpath"))
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

	// Start DEBUG server
	if viper.GetBool("http_pprof") {
		log.Debug("Start DEBUG server")
		go func() { log.Println(http.ListenAndServe("localhost:6060", nil)) }()
	}

	// Enable CPU profiling
	cpuprofile := viper.GetString("cpuprofile")
	if cpuprofile != "" {
		log.Debug("Start cpuprofile")
		f, err := os.Create(cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Enable memory profiling
	memprofile := viper.GetString("memprofile")
	if memprofile != "" {
		log.Debug("Start memprofile")
		f, err := os.Create(memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		runtime.GC() // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
		f.Close()
	}

	// Set clusterName
	clusterName = viper.GetString("cluster_name")

	// Sett parallelism
	runtime.GOMAXPROCS(viper.GetInt("jobs"))

	// Set main dirs var
	archiveDir = viper.GetString("archivedir")
	backupDir = filepath.Join(archiveDir, subDirBasebackup)
	walDir = filepath.Join(archiveDir, subDirWal)
	log.Debug("archiveDir: ", archiveDir)
	log.Debug("backupDir: ", backupDir)
	log.Debug("walDir: ", walDir)

	// TODO we maybe have duplicated entrys in viper. pls fix this
	// Set some variables in Viper,to use them easier in other packages
	viper.SetDefault("waldir", walDir)
	viper.SetDefault("backupdir", backupDir)
	viper.SetDefault("myname", myName)
	viper.SetDefault("version", Version)
	viper.SetDefault("retain", 10)
	vipermap := viper.AllSettings
	for key, value := range vipermap() {
		log.Debugf("%s %s", key, value)
	}

	// Set path for the tools
	cmdTar = viper.GetString("path_to_tar")
	cmdBasebackup = viper.GetString("path_to_basebackup")
	cmdZstd = viper.GetString("path_to_zstd")
	cmdZstdcat = viper.GetString("path_to_zstdcat")
	cmdGpg = viper.GetString("path_to_gpg")

	baseBackupTools = []string{
		cmdTar,
		cmdBasebackup,
		cmdZstd,
		cmdZstdcat,
		cmdGpg,
	}

	// Check if needed tools are available
	err := testTools(baseBackupTools)
	ec.Check(err)

	// Check if the configured backend is supported
	if err := storage.CheckBackend(viper.GetString("backup_to")); err != nil {
		log.Fatal(err)
	}
}

// Global needed functions
func printDone() {
	elapsed := time.Since(startTime)
	log.Info("Done in ", elapsed)
}

// testTools test if all tools in tools are installed by trying to run them
func testTools(tools []string) (err error) {
	if viper.GetBool("no_tool_check") {
		log.Debug("testTools will be ignored because of no_tool_check")
		return nil
	}

	failCounter := 0
	for _, tool := range tools {
		err = testTool(tool)
		if err != nil {
			failCounter++
		}
	}
	if failCounter > 0 {
		return errors.New(strconv.Itoa(failCounter) + " tools seem to be not functional")
	}
	return nil
}

func testTool(tool string) (err error) {
	cmd := exec.Command(tool, "--version")
	var out bytes.Buffer
	cmd.Stdout = &out
	err = cmd.Run()
	if err == nil {
		log.Debug("Tool ", tool, " seems to be functional")
		return nil
	}
	log.Debug("Output of tool: ", tool, " is: ", out.String())
	log.Warning("It seems that tool: ", tool, " is not working correctly: ", err)
	log.Info("You might want to change the path for that tool in the configuration: ", tool)
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
		log.Fatal("Can not get PostgreSQL setting: ", setting, " err:", err)
		return "", err
	}
	log.Debug("Got ", value, " for ", setting, " in pg_settings")
	return value, nil
}

// setPgSetting sets a value to a setting
func setPgSetting(db *sql.DB, setting string, value string) (err error) {
	// TODO Bad style and risk for injection!!! But no better option ... open for suggestions!
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
			log.Warn(p, " is not set")
		}
	}
	if errCount > 0 {
		return errors.New("Not all parameters are set")
	}
	return nil
}

// compressEncryptStream takes a stream and:
// * compresses it
// * endcrypts it (if configured)
// * persists it to given storage backend though storeStream function
func compressEncryptStream(input *io.ReadCloser, name string, storageBackend storeStream, wg *sync.WaitGroup) {
	// Tell the waiting group this process is done when function ends
	defer wg.Done()

	// We are using zstd for compression, add extension
	name = name + ".zst"

	// Are we using encryption?
	encrypt := viper.GetBool("encrypt")
	recipient := viper.GetStringSlice("recipient")

	// This command is used to compress the backup
	compressCmd := exec.Command(cmdZstd)

	// attach pipe to the command
	compressStdout, err := compressCmd.StdoutPipe()
	if err != nil {
		log.Fatal("Can not attach pipe to backup process, ", err)
	}

	// Watch output on stderror
	compressDone := make(chan struct{}) // Channel to wait for WatchOutput
	compressStderror, err := compressCmd.StderrPipe()
	ec.Check(err)
	go util.WatchOutput(compressStderror, log.Info, compressDone)

	// Pipe the backup in the compression
	compressCmd.Stdin = *input

	// Start compression
	if err := compressCmd.Start(); err != nil {
		log.Fatal("zstd failed on startup, ", err)
	}
	log.Info("Compression started")

	// Stream which is send to storage backend
	var dataStream io.Reader

	// Handle encryption
	var gpgCmd *exec.Cmd
	var gpgDone chan struct{} // Channel to wait for WatchOutput
	if encrypt {
		log.Debug("Encrypt data, encrypt: ", encrypt)
		// Encrypt the compressed data
		gpgDone = make(chan struct{})
		gpgArgs := []string{"--encrypt", "-o", "-"}

		// Add all recipients to the command
		for _, r := range recipient {
			gpgArgs = append(gpgArgs, "--recipient", r)
		}

		gpgCmd = exec.Command(cmdGpg, gpgArgs...)
		// Set the encryption output as input for S3
		var err error
		dataStream, err = gpgCmd.StdoutPipe()
		if err != nil {
			log.Fatal("Can not attach pipe to gpg process, ", err)
		}
		// Attach output of WAL to stdin
		gpgCmd.Stdin = compressStdout
		// Watch output on stderror
		gpgStderror, err := gpgCmd.StderrPipe()
		ec.Check(err)
		go util.WatchOutput(gpgStderror, log.Warn, gpgDone)

		// Start encryption
		if err := gpgCmd.Start(); err != nil {
			log.Fatal("gpg failed on startup, ", err)
		}
		log.Debug("gpg started")
	} else {
		// Do not use encryption
		dataStream = compressStdout
	}

	// Store the streamed data
	storageBackend(&dataStream, name)

	// Wait for watch goroutine before Cmd.Wait(), race condition!
	<-compressDone

	// Wait for compression to finish
	// If there is still data in the output pipe it can be lost!
	log.Debug("Wait for compressCmd")
	err = compressCmd.Wait()
	if err != nil {
		log.Fatal("compression failed after startup, ", err)
	}
	log.Debug("compressCmd done")

	// If encryption is used wait for it to finish
	if encrypt {
		log.Debug("Wait for gpgCmd")
		// Wait for output watchers to finish
		// If the Cmd.Wait() is called while another process is reading
		// from Stdout / Stderr this is a race condition.
		// So we are waiting for the watchers first
		<-gpgDone

		// Wait for the command itself
		err = gpgCmd.Wait()
		if err != nil {
			log.Fatal("gpg failed after startup, ", err)
		}
		log.Debug("Encryption done")
	}
}
