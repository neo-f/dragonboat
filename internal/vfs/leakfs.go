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
	"sync/atomic"
	"testing"

	pvfs "github.com/cockroachdb/pebble/v2/vfs"
)

// MemFS is a memory backed file system for testing purposes. It tracks files
// opened via Create, Open, OpenDir, OpenReadWrite and ReuseForWrite so leaked
// file descriptors can be reported by ReportLeakedFD.
type MemFS struct {
	*pvfs.MemFS
	openFiles atomic.Int64
}

var _ IFS = (*MemFS)(nil)

// newMemFS creates a new leak tracking in-memory fs.
func newMemFS() *MemFS {
	return &MemFS{MemFS: pvfs.NewMem()}
}

func (fs *MemFS) track(f File, err error) (File, error) {
	if err != nil {
		return nil, err
	}
	fs.openFiles.Add(1)
	return &trackedFile{File: f, fs: fs}, nil
}

// Create implements IFS.Create.
func (fs *MemFS) Create(name string,
	category pvfs.DiskWriteCategory) (File, error) {
	return fs.track(fs.MemFS.Create(name, category))
}

// Open implements IFS.Open.
func (fs *MemFS) Open(name string, opts ...pvfs.OpenOption) (File, error) {
	return fs.track(fs.MemFS.Open(name, opts...))
}

// OpenDir implements IFS.OpenDir.
func (fs *MemFS) OpenDir(name string) (File, error) {
	return fs.track(fs.MemFS.OpenDir(name))
}

// OpenReadWrite implements IFS.OpenReadWrite.
func (fs *MemFS) OpenReadWrite(name string,
	category pvfs.DiskWriteCategory, opts ...pvfs.OpenOption) (File, error) {
	return fs.track(fs.MemFS.OpenReadWrite(name, category, opts...))
}

// ReuseForWrite implements IFS.ReuseForWrite.
func (fs *MemFS) ReuseForWrite(oldname, newname string,
	category pvfs.DiskWriteCategory) (File, error) {
	return fs.track(fs.MemFS.ReuseForWrite(oldname, newname, category))
}

// trackedFile is a File wrapper that decrements the open file counter of its
// owning MemFS when closed, Close is guarded so double Close is not counted
// twice.
type trackedFile struct {
	File
	fs     *MemFS
	closed atomic.Bool
}

// Close closes the file.
func (f *trackedFile) Close() error {
	if f.closed.CompareAndSwap(false, true) {
		f.fs.openFiles.Add(-1)
	}
	return f.File.Close()
}

// ReportLeakedFD reports leaked file fds.
func ReportLeakedFD(fs IFS, t *testing.T) {
	mf, ok := fs.(*MemFS)
	if !ok {
		return
	}
	if n := mf.openFiles.Load(); n != 0 {
		t.Fatalf("%d file(s) not closed", n)
	}
}
