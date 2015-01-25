package swift

import (
	"fmt"
	"github.com/ncw/swift"
	"github.com/smira/aptly/aptly"
	"github.com/smira/aptly/files"
	"os"
	"path/filepath"
	"strings"
)

// PublishedStorage abstract file system with published files (actually hosted on Swift)
type PublishedStorage struct {
	swift            *swift.Connection
	container        string
	prefix           string
}

// Check interface
var (
	_ aptly.PublishedStorage = (*PublishedStorage)(nil)
)


// NewPublishedStorage creates new instance of PublishedStorage with specified Swift access
// keys, authurl and container name
func NewPublishedStorage(authUrl string, userName string, apiKey string, container string, prefix string) (*PublishedStorage, error) {
	result := &PublishedStorage{
		swift: &swift.Connection{
			UserName: userName,
			ApiKey:   apiKey,
			AuthUrl:  authUrl,
		},
		container: container,
		prefix: prefix,
	}
	if result.prefix == "/" {
		result.prefix = ""
	}
	return result, nil
}

// String
func (storage *PublishedStorage) String() string {
	return fmt.Sprintf("Swift: %s %s %s %s", storage.swift.AuthUrl, storage.swift.UserName, storage.container, storage.prefix)
}

// MkDir creates directory recursively under public path
func (storage *PublishedStorage) MkDir(path string) error {
	// no op for Swift
	return nil
}

// PutFile puts file into published storage at specified path
func (storage *PublishedStorage) PutFile(path string, sourceFilename string) error {
	var (
		source *os.File
		err    error
	)
	source, err = os.Open(sourceFilename)
	if err != nil {
		return err
	}
	defer source.Close()

	_, err = storage.swift.ObjectPut(storage.container, filepath.Join(storage.prefix, path), source, false, "", "binary/octet-stream", nil)
	if err != nil {
		return fmt.Errorf("error uploading %s to %s: %s", sourceFilename, storage, err)
	}

	return nil
}

// Remove removes single file under public path
func (storage *PublishedStorage) Remove(path string) error {
	err := storage.swift.ObjectDelete(storage.container, filepath.Join(storage.prefix, path))
	if err != nil {
		return fmt.Errorf("error deleting %s from %s: %s", path, storage, err)
	}
	return nil
}

// RemoveDirs removes directory structure under public path
func (storage *PublishedStorage) RemoveDirs(path string, progress aptly.Progress) error {

	filelist, err := storage.Filelist(path)
	if err != nil {
		return err
	}

	for _, filename := range filelist {
		err = storage.Remove(filename)
		if err != nil {
			return err
		}
	}

	return nil
}

// LinkFromPool links package file from pool to dist's pool location
//
// publishedDirectory is desired location in pool (like prefix/pool/component/liba/libav/)
// sourcePool is instance of aptly.PackagePool
// sourcePath is filepath to package file in package pool
//
// LinkFromPool returns relative path for the published file to be included in package index
func (storage *PublishedStorage) LinkFromPool(publishedDirectory string, sourcePool aptly.PackagePool,
	sourcePath, sourceMD5 string, force bool) error {
	// verify that package pool is local pool in filesystem
	_ = sourcePool.(*files.PackagePool)

	baseName := filepath.Base(sourcePath)
	relPath := filepath.Join(publishedDirectory, baseName)
	poolPath := filepath.Join(storage.prefix, relPath)

	object, _, err := storage.swift.Object(storage.container, poolPath)
	if err != nil {
		if err != swift.ObjectNotFound {
			return fmt.Errorf("error getting information about %s from %s: %s", poolPath, storage, err)
		}
	} else {
		destinationMD5 := strings.Replace(object.Hash, "\"", "", -1)
		if destinationMD5 == sourceMD5 {
			return nil
		}

		if !force && destinationMD5 != sourceMD5 {
			return fmt.Errorf("error putting file to %s: file already exists and is different: %s", poolPath, storage)

		}
	}

	return storage.PutFile(relPath, sourcePath)
}

// Filelist returns list of files under prefix
func (storage *PublishedStorage) Filelist(prefix string) ([]string, error) {
	result := []string{}
	prefix = filepath.Join(storage.prefix, prefix)
	if prefix != "" {
		prefix += "/"
	}
	objects, err := storage.swift.ObjectsAll(storage.container,nil)
	if err != nil {
		return nil, fmt.Errorf("error listing under prefix %s in %s: %s", prefix, storage, err)
	}
	for _, object := range objects {
		result = append(result, object.Name)
	}

	return result, nil
}

// RenameFile renames (moves) file
func (storage *PublishedStorage) RenameFile(oldName, newName string) error {
	return storage.swift.ObjectMove(storage.container, filepath.Join(storage.prefix, oldName),
		storage.container, filepath.Join(storage.prefix, newName))
}
