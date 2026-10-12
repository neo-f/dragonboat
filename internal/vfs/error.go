// Copyright 2017-2020 Lei Ni (nilei81@gmail.com) and other contributors.
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
	"errors"
	"io"
	"os"
	"sync/atomic"

	pvfs "github.com/cockroachdb/pebble/v2/vfs"
)

// ErrInjected is an error injected for testing purposes.
var ErrInjected = errors.New("injected error")

// Op is an enum describing the type of FS operations.
type Op int

const (
	// OpRead describes read operations
	OpRead Op = iota
	// OpWrite describes write operations
	OpWrite
	// OpSync describes the fsync operation
	OpSync
)

// OnIndex creates and returns an injector instance that returns an ErrInjected
// on the (n+1)-th invocation of its MaybeError function.
func OnIndex(index int32, op Op) *InjectIndex {
	inj := &InjectIndex{op: op}
	inj.index.Store(index)
	return inj
}

// InjectIndex implements Injector, injecting an error at a specific index.
type InjectIndex struct {
	index atomic.Int32
	op    Op
}

// Index returns the index at which the error will be injected.
func (ii *InjectIndex) Index() int32 { return ii.index.Load() }

// SetIndex sets the index at which the error will be injected.
func (ii *InjectIndex) SetIndex(v int32) { ii.index.Store(v) }

// MaybeError implements the Injector interface.
func (ii *InjectIndex) MaybeError(op Op) error {
	if ii.op != op {
		return nil
	}
	if ii.index.Add(-1) == -1 {
		return ErrInjected
	}
	return nil
}

// Injector injects errors into FS.
type Injector interface {
	MaybeError(Op) error
}

var _ IFS = &ErrorFS{}

// ErrorFS is a vfs.FS implementation, injecting errors into its operations.
type ErrorFS struct {
	fs  IFS
	inj Injector
}

// Wrap wraps an existing IFS implementation with the specified injector. If an
// error is injected, ErrorFS propagates the error instead of shadowing the
// operation.
func Wrap(fs IFS, inj Injector) *ErrorFS {
	return &ErrorFS{
		fs:  fs,
		inj: inj,
	}
}

// Unwrap returns the FS implementation underlying fs.
func (fs *ErrorFS) Unwrap() IFS {
	return fs.fs
}

// Create implements IFS.Create.
func (fs *ErrorFS) Create(name string,
	category pvfs.DiskWriteCategory) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	f, err := fs.fs.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// Link implements IFS.Link.
func (fs *ErrorFS) Link(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Link(oldname, newname)
}

// Open implements IFS.Open.
func (fs *ErrorFS) Open(name string, opts ...pvfs.OpenOption) (File, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	f, err := fs.fs.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// OpenDir implements IFS.OpenDir.
func (fs *ErrorFS) OpenDir(name string) (File, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// PathBase implements IFS.PathBase.
func (fs *ErrorFS) PathBase(p string) string {
	return fs.fs.PathBase(p)
}

// PathDir implements IFS.PathDir.
func (fs *ErrorFS) PathDir(p string) string {
	return fs.fs.PathDir(p)
}

// PathJoin implements IFS.PathJoin.
func (fs *ErrorFS) PathJoin(elem ...string) string {
	return fs.fs.PathJoin(elem...)
}

// Remove implements IFS.Remove.
func (fs *ErrorFS) Remove(name string) error {
	if _, err := fs.fs.Stat(name); os.IsNotExist(err) {
		return nil
	}

	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Remove(name)
}

// RemoveAll implements IFS.RemoveAll.
func (fs *ErrorFS) RemoveAll(fullname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.RemoveAll(fullname)
}

// Rename implements IFS.Rename.
func (fs *ErrorFS) Rename(oldname, newname string) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.Rename(oldname, newname)
}

// ReuseForWrite implements IFS.ReuseForWrite.
func (fs *ErrorFS) ReuseForWrite(oldname, newname string,
	category pvfs.DiskWriteCategory) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	return fs.fs.ReuseForWrite(oldname, newname, category)
}

// OpenReadWrite implements IFS.OpenReadWrite.
func (fs *ErrorFS) OpenReadWrite(name string,
	category pvfs.DiskWriteCategory, opts ...pvfs.OpenOption) (File, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	f, err := fs.fs.OpenReadWrite(name, category, opts...)
	if err != nil {
		return nil, err
	}
	return &errorFile{f, fs.inj}, nil
}

// MkdirAll implements IFS.MkdirAll.
func (fs *ErrorFS) MkdirAll(dir string, perm os.FileMode) error {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return err
	}
	return fs.fs.MkdirAll(dir, perm)
}

// Lock implements IFS.Lock.
func (fs *ErrorFS) Lock(name string) (io.Closer, error) {
	if err := fs.inj.MaybeError(OpWrite); err != nil {
		return nil, err
	}
	return fs.fs.Lock(name)
}

// List implements IFS.List.
func (fs *ErrorFS) List(dir string) ([]string, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return fs.fs.List(dir)
}

// Stat implements IFS.Stat.
func (fs *ErrorFS) Stat(name string) (pvfs.FileInfo, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return fs.fs.Stat(name)
}

// GetDiskUsage implements IFS.GetDiskUsage.
func (fs *ErrorFS) GetDiskUsage(path string) (pvfs.DiskUsage, error) {
	if err := fs.inj.MaybeError(OpRead); err != nil {
		return pvfs.DiskUsage{}, err
	}
	return fs.fs.GetDiskUsage(path)
}

var _ File = &errorFile{}

// errorFile implements vfs.File. The interface is implemented on the pointer
// type to allow pointer equality comparisons.
type errorFile struct {
	file File
	inj  Injector
}

func (f *errorFile) Close() error {
	// We don't inject errors during close as those calls should never fail in
	// practice.
	return f.file.Close()
}

func (f *errorFile) Read(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return 0, err
	}
	return f.file.Read(p)
}

func (f *errorFile) ReadAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return 0, err
	}
	return f.file.ReadAt(p, off)
}

func (f *errorFile) Write(p []byte) (int, error) {
	if err := f.inj.MaybeError(OpWrite); err != nil {
		return 0, err
	}
	return f.file.Write(p)
}

func (f *errorFile) WriteAt(p []byte, off int64) (int, error) {
	if err := f.inj.MaybeError(OpWrite); err != nil {
		return 0, err
	}
	return f.file.WriteAt(p, off)
}

func (f *errorFile) Preallocate(offset, length int64) error {
	return f.file.Preallocate(offset, length)
}

func (f *errorFile) Stat() (pvfs.FileInfo, error) {
	if err := f.inj.MaybeError(OpRead); err != nil {
		return nil, err
	}
	return f.file.Stat()
}

func (f *errorFile) Sync() error {
	if err := f.inj.MaybeError(OpSync); err != nil {
		return err
	}
	return f.file.Sync()
}

func (f *errorFile) SyncData() error {
	if err := f.inj.MaybeError(OpSync); err != nil {
		return err
	}
	return f.file.SyncData()
}

func (f *errorFile) SyncTo(length int64) (bool, error) {
	if err := f.inj.MaybeError(OpSync); err != nil {
		return false, err
	}
	return f.file.SyncTo(length)
}

func (f *errorFile) Prefetch(offset, length int64) error {
	return f.file.Prefetch(offset, length)
}

func (f *errorFile) Fd() uintptr {
	return f.file.Fd()
}
