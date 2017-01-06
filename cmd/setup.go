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
	"database/sql"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

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
	// PostgreSQL settings
	pgSettings = map[string]string{
		"archive_command": "",
		"archive_mode":    "",
		"wal_level":       "",
		"max_wal_senders": "",
	}

	// Tools that should be installed
	setupTools = []string{
		cmdZstd,
	}

	// If enabled: dry run
	dryRun = false

	// All directories that should be created if missing
	subDirs = []string{subDirBasebackup, subDirWal}

	setupCmd = &cobra.Command{
		Use:   "setup",
		Short: "Setup PostgreSQL and needed directories.",
		Long:  `This command makes all needed configuration changes via ALTER SYSTEM and creates missing folders. To operate it needs a superuser connection (connection sting) and the path where the backups should go.`,
		Run: func(cmd *cobra.Command, args []string) {
			log.Info("Run Setup")

			// Check if needed tools are available
			err := testTools(setupTools)
			check(err)

			// When no archive command set, set it
			if viper.GetString("archive_command") == "" {
				// Include config file in potential archive command
				configOption := ""
				if viper.ConfigFileUsed() != "" {
					configOption = " --config " + viper.ConfigFileUsed()
				}

				// Preset archive_command
				viper.Set("archive_command", myExecutable+configOption+" archive %p")
			}

			// Check if we perform a dry run
			dryRun = viper.GetBool("check")
			if dryRun == true {
				log.Info("Running in dry run mode, nothing is changed!")
			}

			// Fill up pgSettings
			pgSettings["archive_command"] = viper.GetString("archive_command")
			pgSettings["archive_mode"] = viper.GetString("archive_mode")
			pgSettings["wal_level"] = viper.GetString("wal_level")
			pgSettings["max_wal_senders"] = viper.GetString("max_wal_senders")

			// Connect to database
			conString := viper.GetString("connection")
			log.Debug("Using the following connection string: ", conString)
			db, err := sql.Open("postgres", conString)
			if err != nil {
				log.Fatal("Unable to connect to database!")
			}
			defer db.Close()

			// Get pg_data
			pgData, err := getPgData(db)

			// Get and check version via SQL
			pgVersion, err := checkPgVersion(db)
			check(err)

			// Get version of the data
			pgDataVersion, err := getMajorVersionFromPgData(pgData)
			check(err)

			log.WithFields(log.Fields{
				"pgData":           pgData,
				"archiveDir":       archiveDir,
				"pgVersion.string": pgVersion.string,
				"pgVersion.num":    pgVersion.num,
				"pgDataVersion":    pgDataVersion,
			}).Debug("Variables")

			log.Debug(pgSettings)

			if dryRun == true {
				log.Info("Dry run ends here, now the setup would happen.")
				os.Exit(0)
			}

			// Create directories for backups, WAL and configuration
			err = createDirs(archiveDir, subDirs)
			check(err)

			// Configure PostgreSQL for archiving
			log.Info("Configure PostgreSQL for archiving.")
			changed, _ := configurePostgreSQL(db, pgSettings)
			check(err)
			// If more than 0 setings have been changed we reload the configuration
			if changed > 0 {
				log.Info("Going to reload the configuration.")
				reloadConfiguration(db)

				// Configure PostgreSQL again to see if all settings are good now!
				changed, _ = configurePostgreSQL(db, pgSettings)
				check(err)
			}

			if changed > 0 {
				// Settings are still not good, restart needed!
				log.Warn("Not all settings took effect, we need to restart the Database!")
				pgRestartDB(pgData)
				if err != nil {
					log.Fatal("Unable to restart Database: ", err)
				}
			}
			log.Info("PostgreSQL is configured for archiving.")
			printDone()
		},
	}
)

func init() {
	RootCmd.AddCommand(setupCmd)

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	setupCmd.PersistentFlags().String("archive_command", "", "The command to archive WAL files")
	setupCmd.PersistentFlags().String("archive_mode", "on", "The archive mode (should be 'on' to archive)")
	setupCmd.PersistentFlags().String("wal_level", "hot_standby", "The level of information to include in WAL files")
	setupCmd.PersistentFlags().String("max_wal_senders", "3", "The max number of walsender processes")
	setupCmd.PersistentFlags().Bool("check", false, "Perform only a dry run without doing changes")

	// Bind flags to viper
	viper.BindPFlag("archive_command", setupCmd.PersistentFlags().Lookup("archive_command"))
	viper.BindPFlag("archive_mode", setupCmd.PersistentFlags().Lookup("archive_mode"))
	viper.BindPFlag("wal_level", setupCmd.PersistentFlags().Lookup("wal_level"))
	viper.BindPFlag("max_wal_senders", setupCmd.PersistentFlags().Lookup("max_wal_senders"))
	viper.BindPFlag("check", setupCmd.PersistentFlags().Lookup("check"))
}

// pgRestartDB is called when PostgreSQL needs a restart
// it then shows the user the need to restart PostgreSQL
func pgRestartDB(pgData string) (err error) {
	postmasterPID, err := getPostmasterPID(pgData)
	check(err)
	log.Warn("Please restart PostgreSQL wth PID ", postmasterPID)
	return err
}

func isMajorVersionSupported(pgMjaorVersion string) (supported bool) {
	for _, version := range supportedMajorVersions {
		if pgMjaorVersion == version {
			return true
		}
	}
	return false
}

// isPgVersionSupported returns true if pgVersionNum is supported
func isPgVersionSupported(pgVersionNum int) (supported bool) {
	if pgVersionNum < pgMinVersion {
		log.Warning("The version of PostgreSQL ist too old and not supported! Your version: ", pgVersionNum, " Min required version: ", pgMinVersion)
		return false
	}

	if pgVersionNum > pgMaxVersion {
		log.Warning("The version of PostgreSQL is not jet support! Your version: ", pgVersionNum, " Max supported version: ", pgMaxVersion)
		return false
	}

	return true
}

// configurePostgreSQL set all settings in "settings" return count of changes
func configurePostgreSQL(db *sql.DB, settings map[string]string) (changed int, err error) {
	changed = 0
	for setting := range settings {
		settingShould := settings[setting]
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

// createDirs creates all dirs in archivedir + "/" + subDirs
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
