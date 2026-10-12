// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package vfs

import (
	"os"
	"path/filepath"

	"github.com/cockroachdb/errors/oserror"
	pvfs "github.com/cockroachdb/pebble/v2/vfs"
)

// IFS is the vfs interface used by dragonboat.
type IFS = pvfs.FS

// File is the file interface returned by IFS.
type File = pvfs.File

// WriteCategoryUnspecified denotes a disk write without a significant
// category.
const WriteCategoryUnspecified = pvfs.WriteCategoryUnspecified

// DefaultFS is a vfs instance using underlying OS fs.
var DefaultFS IFS = pvfs.Default

// MemStrictFS is a vfs instance using memfs.
var MemStrictFS IFS = NewMemFS()

// NewMemFS creates a in-memory fs.
func NewMemFS() IFS {
	return newMemFS()
}

// OpenForAppend opens the named file for appending. The returned file
// provides single-writer sequential-append semantics, Write calls are
// translated into WriteAt calls at the advancing end of the file.
func OpenForAppend(fs IFS, name string) (File, error) {
	f, err := fs.OpenReadWrite(name, WriteCategoryUnspecified)
	if err != nil {
		return nil, err
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	return &appendFile{File: f, offset: fi.Size()}, nil
}

// appendFile is a File wrapper providing single-writer sequential-append
// semantics on top of a regular read-write file.
type appendFile struct {
	File
	offset int64
}

// Write appends the specified data to the end of the file.
func (f *appendFile) Write(p []byte) (int, error) {
	n, err := f.WriteAt(p, f.offset)
	f.offset += int64(n)
	return n, err
}

// IsNotExist returns a boolean value indicating whether the specified error is
// to indicate that a file or directory does not exist.
func IsNotExist(err error) bool {
	return oserror.IsNotExist(err)
}

// IsExist returns a boolean value indicating whether the specified error is to
// indicate that a file or directory already exists.
func IsExist(err error) bool {
	return oserror.IsExist(err)
}

// TempDir returns the directory use for storing temporary files.
func TempDir() string {
	return os.TempDir()
}

// Clean is a wrapper for filepath.Clean.
func Clean(dir string) string {
	return filepath.Clean(dir)
}
