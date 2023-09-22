package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
	"go.etcd.io/bbolt"
)

// BackupBBolt backs up a bbolt database using a read only transaction into the
// same directory. The backup is named with the current unix timestamp.
func BackupBBolt(db *bbolt.DB, backupFrequency, backupCount int) error {
	// ---------------------------
	dbDir := filepath.Dir(db.Path())
	dbName := filepath.Base(db.Path())
	// ---------------------------
	dirContent, err := os.ReadDir(dbDir)
	if err != nil {
		return fmt.Errorf("could not read directory: %w", err)
	}
	// ---------------------------
	// List all backup files
	// We do -1 because the last file is the current database file
	backupFiles := make([]string, 0, len(dirContent)-1)
	for _, dirEntry := range dirContent {
		if strings.HasSuffix(dirEntry.Name(), ".backup") {
			backupFiles = append(backupFiles, dirEntry.Name())
		}
	}
	slices.Sort(backupFiles) // in ascending order
	// ---------------------------
	// We need to keep track of all errors that occur during the backup process.
	// We do this to still function in case we can't read the latest timestamp
	// or continue cleaning up old backups if we can't delete a backup file.
	errs := make([]error, 0)
	// ---------------------------
	// Get latest backup time to see if we need to create a new backup
	currentUnixTime := time.Now().Unix()
	lastestBackupTime := int64(0)
	if len(backupFiles) > 0 {
		lastBackupName := backupFiles[len(backupFiles)-1]
		lastestBackupTime, err = strconv.ParseInt(strings.Split(lastBackupName, "-")[0], 10, 64)
		if err != nil {
			errs = append(errs, fmt.Errorf("could not parse latest backup file name %s: %w", lastBackupName, err))
		}
	}
	// ---------------------------
	// Perform backup if the last backup is older than the minimum backup frequency
	if currentUnixTime-lastestBackupTime >= int64(backupFrequency) {
		backupFile := filepath.Join(dbDir, fmt.Sprintf("%v-%s.backup", currentUnixTime, dbName))
		err := db.View(func(tx *bbolt.Tx) error {
			return tx.CopyFile(backupFile, 0644)
		})
		if err != nil {
			errs = append(errs, fmt.Errorf("could not create backup: %w", err))
		} else {
			log.Debug().Str("component", "backupBBolt").Str("backupFile", backupFile).Msg("Created backup")
			backupFiles = append(backupFiles, backupFile)
		}
	}
	// ---------------------------
	// Clean up old backup files by keeping only the last N backups
	for i := 0; i < len(backupFiles)-backupCount; i++ {
		backupFile := filepath.Join(dbDir, backupFiles[i])
		if err := os.Remove(backupFile); err != nil {
			errs = append(errs, fmt.Errorf("could not delete backup file %s: %w", backupFile, err))
		}
	}
	// ---------------------------
	return errors.Join(errs...)
}
