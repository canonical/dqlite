package connection

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// Snapshot returns a snapshot of the SQLite database with the given path.
//
// The snapshot is comprised of two byte slices, one with the content of the
// database and one is the content of the WAL file.
func Snapshot(path string) ([]byte, []byte, error) {
	// Create a source connection that will read the database snapshot.
	sourceConn, err := open(path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "source connection")
	}
	defer sourceConn.Close()

	// Create a backup connection that will write the database snapshot.
	backupPath, err := newBackupPath(path)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create temp file for backup")
	}
	defer os.Remove(backupPath)
	backupConn, err := open(backupPath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to open backup connection")
	}

	// Cleanup the shm and wal files of the backup as well.
	defer os.Remove(backupPath + "-wal")
	defer os.Remove(backupPath + "-shm")
	defer backupConn.Close()

	// Perform the backup.
	backup, err := backupConn.Backup("main", sourceConn, "main")
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to init backup database")
	}
	done, err := backup.Step(-1)
	backup.Close()
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to backup database")
	}
	if !done {
		return nil, nil, fmt.Errorf("database backup not complete")
	}

	// Read the backup database and WAL.
	database, err := ioutil.ReadFile(backupPath)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "cannot read backup content at %s", backupPath)
	}

	wal, err := ioutil.ReadFile(backupPath + "-wal")
	if err != nil {
		return nil, nil, err
	}

	return database, wal, nil
}

// Restore the given database and WAL backups, writing them at the given
// database path.
func Restore(path string, database []byte, wal []byte) error {
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove current database")
	}
	if err := ioutil.WriteFile(path, database, 0600); err != nil {
		return errors.Wrapf(err, "failed to write database content at %s", path)
	}

	if err := os.Remove(path + "-wal"); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove current WAL")
	}
	if err := ioutil.WriteFile(path+"-wal", wal, 0600); err != nil {
		return errors.Wrapf(err, "failed to write wal content at %s", path)
	}

	if err := os.Remove(path + "-shm"); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove current shm file")
	}

	return nil
}

// Return the path to a temporary file that will be used to write the backup of
// the database being snapshotted.
//
// The temporary file lives in the same directory as the database being
// snapshotted and its named after its filename.
func newBackupPath(path string) (string, error) {
	// Create a temporary file using the source DSN filename as prefix.
	file, err := ioutil.TempFile(filepath.Dir(path), filepath.Base(path)+"-")
	if err != nil {
		return "", err
	}
	defer file.Close()

	return file.Name(), nil
}
