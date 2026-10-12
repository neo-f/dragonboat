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

package server

import (
	"testing"

	"github.com/neo-f/dragonboat/v4/internal/vfs"
	pb "github.com/neo-f/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testHomeDir = "/home/test"

func reportLeakedFD(t *testing.T, fs vfs.IFS) {
	t.Helper()
	vfs.ReportLeakedFD(t, fs)
}

func TestGetSnapshotDirName(t *testing.T) {
	v := GetSnapshotDirName(1)
	assert.Equal(t, "snapshot-0000000000000001", v)
	v = GetSnapshotDirName(255)
	assert.Equal(t, "snapshot-00000000000000FF", v)
}

func TestMustBeChild(t *testing.T) {
	tests := []struct {
		parent string
		child  string
		ok     bool
	}{
		{testHomeDir, "/home", false},
		{testHomeDir, testHomeDir, false},
		{testHomeDir, "/home/data", false},
		{testHomeDir, "/home/test1", false},
		{testHomeDir, "/home/test/data", true},
		{testHomeDir, "", false},
	}
	for _, tt := range tests {
		parent := tt.parent
		child := tt.child
		expectedOk := tt.ok

		if expectedOk {
			assert.NotPanics(t, func() {
				mustBeChild(parent, child)
			})
		} else {
			require.Panics(t, func() {
				mustBeChild(parent, child)
			})
		}
	}
}

func TestTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshotMode, fs)
	dir := env.GetTempDir()
	assert.Contains(t, dir, ".generating")
	env = NewSSEnv(f, 1, 1, 1, 2, ReceivingMode, fs)
	dir = env.GetTempDir()
	assert.Contains(t, dir, ".receiving")
	reportLeakedFD(t, fs)
}

func TestFinalSnapshotDirDoesNotContainTempSuffix(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshotMode, fs)
	dir := env.GetFinalDir()
	assert.NotContains(t, dir, ".generating")
}

func TestRootDirIsTheParentOfTempFinalDirs(t *testing.T) {
	f := func(cid uint64, nid uint64) string {
		return "/data"
	}
	fs := vfs.GetTestFS()
	env := NewSSEnv(f, 1, 1, 1, 2, SnapshotMode, fs)
	tmpDir := env.GetTempDir()
	finalDir := env.GetFinalDir()
	rootDir := env.GetRootDir()
	assert.NotPanics(t, func() {
		mustBeChild(rootDir, tmpDir)
	})
	assert.NotPanics(t, func() {
		mustBeChild(rootDir, finalDir)
	})
	reportLeakedFD(t, fs)
}

func runEnvTest(t *testing.T, f func(t *testing.T, env SSEnv),
	fs vfs.IFS) {
	t.Helper()
	rd := "server-pkg-test-data-safe-to-delete"
	defer func() {
		err := fs.RemoveAll(rd)
		require.NoError(t, err)
	}()
	func() {
		ff := func(cid uint64, nid uint64) string {
			return rd
		}
		env := NewSSEnv(ff, 1, 1, 1, 2, SnapshotMode, fs)
		tmpDir := env.GetTempDir()
		err := fs.MkdirAll(tmpDir, 0755)
		require.NoError(t, err)
		f(t, env)
	}()
	reportLeakedFD(t, fs)
}

func TestRenameTempDirToFinalDir(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		t.Helper()
		err := env.renameToFinalDir()
		require.NoError(t, err)
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestRenameTempDirToFinalDirCanComplete(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		t.Helper()
		assert.False(t, env.finalDirExists())
		err := env.renameToFinalDir()
		require.NoError(t, err)
		assert.True(t, env.finalDirExists())
		assert.False(t, env.HasFlagFile())
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFlagFileExists(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		t.Helper()
		assert.False(t, env.finalDirExists())
		msg := &pb.Message{}
		err := env.createFlagFile(msg)
		require.NoError(t, err)
		err = env.renameToFinalDir()
		require.NoError(t, err)
		assert.True(t, env.finalDirExists())
		assert.True(t, env.HasFlagFile())
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFinalizeSnapshotCanComplete(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		t.Helper()
		m := &pb.Message{}
		err := env.FinalizeSnapshot(m)
		require.NoError(t, err)
		assert.True(t, env.HasFlagFile())
		assert.True(t, env.finalDirExists())
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}

func TestFinalizeSnapshotReturnOutOfDateWhenFinalDirExist(t *testing.T) {
	tf := func(t *testing.T, env SSEnv) {
		t.Helper()
		finalDir := env.GetFinalDir()
		err := env.fs.MkdirAll(finalDir, 0755)
		require.NoError(t, err)
		m := &pb.Message{}
		err = env.FinalizeSnapshot(m)
		assert.Equal(t, ErrSnapshotOutOfDate, err)
		assert.False(t, env.HasFlagFile())
	}
	fs := vfs.GetTestFS()
	runEnvTest(t, tf, fs)
}
