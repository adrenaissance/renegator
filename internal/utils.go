package internal

import (
	"crypto/sha256"
	"encoding/hex"
	_"fmt"
	"os"
)

const (
	defaultMigrationsFolderName = "migrations"
)

// check if the migrations folder exists
// if not return an error
func CheckMigrationsFolder(migrationsFolder *string) error {
	_, err := os.Open(*migrationsFolder)
	if err != nil {
		// create the migrations folder at the ./migrations path
		if migrationsFolder == nil {
			*migrationsFolder = defaultMigrationsFolderName
		}
		err2 := os.Mkdir(*migrationsFolder, 0777)
		if err2 != nil {
			return err2
		}
	}
	return nil
}

// calculate the checksum for the file
func calculate_checksum(filePath string) (string, error) {
    content, err := os.ReadFile(filePath)
    if err != nil {
        return "", err
    }
    hash := sha256.Sum256(content)
    return hex.EncodeToString(hash[:]), nil
}
