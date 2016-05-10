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
	"database/sql"
	"os"
	"os/exec"
	"strconv"

	"github.com/spf13/cobra"

	log "github.com/Sirupsen/logrus"

	// This is needed but never directly called
	_ "github.com/lib/pq"
)

type pgVersion struct {
	string string
	num    int
}

// setupCmd represents the setup command
var (
	// Minimal and maximal PostgreSQL version (numeric)
	pgMinVersion = 90500
	pgMaxVersion = 90599

	// PostgreSQL settings
	pgSettings = map[string]*string{
		"archive_command": new(string),
		"archive_mode":    new(string),
		"wal_level":       new(string),
	}

	// Alle directories that should be created if missing
	subDirs = []string{"current", "base", "wal"}

	setupCmd = &cobra.Command{
		Use:   "setup",
		Short: "Setup PostgreSQL and needed directories.",
		Long:  `This command makes all needed configuration changes via ALTER SYSTEM and creates missing folders. To operate it needs a superuser connection (connection sting) and the path where the backups should go.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("Run Setup")

			// Read config
			// ...

			// Create directories for backups, WAL and configuration
			err := createDirs(archiveDir, subDirs)
			check(err)

			// Connect to database
			conString := "user=postgres dbname=postgres password=toor"
			log.Debug("Connection string, conString:", conString)
			db, err := sql.Open("postgres", conString)
			if err != nil {
				log.Fatal("Unable to connect to database!")
			}
			defer db.Close()

			// Get version
			_, err = checkPgVersion(db)
			check(err)

			// Configure PostgreSQL for archiving
			log.Info("Configure PostgreSQL for archiving.")
			changed, _ := configurePostgreSQL(db, pgSettings)
			check(err)
			if changed > 0 {
				// Configure PostgreSQL again to see if all settings are good now!
				changed, _ = configurePostgreSQL(db, pgSettings)
				check(err)
			}

			if changed > 0 {
				// Settings are still not good, restart needed!
				log.Warn("Not all settings took affect, we need to restart the Database!")
				pgRestartDB(pgDataDir)
				if err != nil {
					log.Fatal("Unable to restart Database: ", err)
				}
			}
			log.Info("PostgreSQl is configured for archiving.")
		},
	}
)

func init() {
	RootCmd.AddCommand(setupCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// pgSettings["archive_command"] = *setupCmd.PersistentFlags().String("archive_command", "test ! -f "+archiveDir+"/wal/%f && cp %p "+archiveDir+"/wal/%f && /bin/sync --data "+archiveDir+"/wal/%f", "The command to archive WAL files")
	setupCmd.PersistentFlags().StringVar(pgSettings["archive_command"], "archive_command", "test ! -f "+archiveDir+"/wal/%f.lzo && lzop -o "+archiveDir+"/wal/%f.lzo %p && /bin/sync --data "+archiveDir+"/wal/%f.lzo", "The command to archive WAL files")
	setupCmd.PersistentFlags().StringVar(pgSettings["archive_mode"], "archive_mode", "on", "The archive mode (should be 'on' to archive)")
	setupCmd.PersistentFlags().StringVar(pgSettings["wal_level"], "wal_level", "hot_standby", "The level of information to include in WAL files")
}

func check(err error) error {
	if err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

func getPgSetting(db *sql.DB, setting string) (value string, err error) {
	query := "SELECT setting FROM pg_settings WHERE name = $1;"
	rows, err := db.Query(query, setting)
	check(err)
	for rows.Next() {
		err = rows.Scan(&value)
		if err != nil {
			log.Fatal("Can't get PostgreSQL setting: ", setting, err)
			return "", err
		}
	}
	return value, nil
}

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

func getPostmasterPID(pgDataDir string) (postmasterPID int, err error) {
	pidFile := pgDataDir + "/postmaster.pid"
	postmasterPID = -1
	file, err := os.Open(pidFile)
	if err != nil {
		log.Error("Can not open PID file ", pidFile)
	}

	scanner := bufio.NewScanner(file)

	// Read first line
	scanner.Scan()
	line := scanner.Text()

	postmasterPID, err = strconv.Atoi(line)
	if err != nil {
		log.Error("Can not parse postmaster PID: ", string(line), " from: ", pidFile)
	}
	return postmasterPID, err
}

func pgRestartDB(pgDataDir string) (err error) {
	postmasterPID, err := getPostmasterPID(pgDataDir)
	log.Warn("Please restart PostgreSQL wth PID ", postmasterPID)
	return err
}

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

	if pgVersion.num < pgMinVersion {
		log.Fatal("The version of PostgreSQL ist too old and not supported! Your version: ", pgVersion.num, " Min required version: ", pgMinVersion)
	}

	if pgVersion.num > pgMaxVersion {
		log.Fatal("The version of PostgreSQL is not jet support! Your version: ", pgVersion.num, " Max supported version: ", pgMaxVersion)
	}
	return pgVersion, err
}

func configurePostgreSQL(db *sql.DB, settings map[string]*string) (changed int, err error) {
	changed = 0
	for setting := range settings {
		settingShould := *settings[setting]
		settingIs, err := getPgSetting(db, setting)
		check(err)
		log.Debug(setting, " should be: ", settingShould, " it is: ", settingIs)

		if settingIs != settingShould {
			err := setPgSetting(db, setting, settingShould)
			check(err)
			changed++
		}
	}
	log.Debug("configurePostgreSQL changed: ", changed, " settings.")
	return changed, nil
}

func testTools(tools []string) {
	for _, tool := range tools {
		cmd := exec.Command("command", "-v", tool)
		err := cmd.Run()
		check(err)
	}
}

func createDirs(archivedir string, subDirs []string) error {
	for _, dir := range subDirs {
		path := archivedir + "/" + dir
		err := os.MkdirAll(path, 0770)
		if err != nil {
			log.Fatal("Can not create directory: ", path)
			return err
		}
	}
	return nil
}
