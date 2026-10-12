// Copyright 2017-2021 Lei Ni (nilei81@gmail.com) and other contributors.
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

package rsm

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"io"
	"testing"

	"github.com/neo-f/dragonboat/v4/internal/vfs"
	pb "github.com/neo-f/dragonboat/v4/raftpb"
	"github.com/stretchr/testify/require"
)

const (
	testSessionSize uint64 = 16
	testPayloadSize uint64 = 8
	testF1DataPath         = "/d1/d2/f1.data"
)

func reportLeakedFD(t *testing.T, fs vfs.IFS) {
	t.Helper()
	vfs.ReportLeakedFD(t, fs)
}

// snapshotTestDir returns a per-test directory, creating it in fs first as
// t.TempDir only creates the directory in the OS fs while tests may run on
// an in-memory fs.
func snapshotTestDir(t *testing.T, fs vfs.IFS) string {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, fs.MkdirAll(dir, 0755))
	return dir
}

func TestSnapshotWriterCanBeCreated(t *testing.T) {
	fs := vfs.GetTestFS()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	w, err := NewSnapshotWriter(fp, pb.NoCompression, fs)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, w.Close())
	}()
}

func TestSaveHeaderSavesTheHeader(t *testing.T) {
	fs := vfs.GetTestFS()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	func() {
		w, err := NewSnapshotWriter(fp, pb.NoCompression, fs)
		require.NoError(t, err)
		sessionData := make([]byte, testSessionSize)
		storeData := make([]byte, testPayloadSize)
		_, err = rand.Read(sessionData)
		require.NoError(t, err)
		_, err = rand.Read(storeData)
		require.NoError(t, err)
		n, err := w.Write(sessionData)
		require.NoError(t, err)
		require.Equal(t, len(sessionData), n)
		m, err := w.Write(storeData)
		require.NoError(t, err)
		require.Equal(t, len(storeData), m)
		err = w.Close()
		require.NoError(t, err)
		r, header, err := NewSnapshotReader(fp, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, r.Close())
		}()
		require.Equal(t, DefaultVersion, SSVersion(header.Version))
		require.Equal(t, DefaultChecksumType, header.ChecksumType)
		storeChecksum := w.vw.GetPayloadSum()
		require.True(t, bytes.Equal(header.PayloadChecksum, storeChecksum))
	}()
	reportLeakedFD(t, fs)
}

func makeTestSnapshotFile(t *testing.T, ssz uint64,
	psz uint64, v SSVersion, fn string, fs vfs.IFS) (*SnapshotWriter, []byte, []byte) {
	t.Helper()
	w, err := newVersionedSnapshotWriter(fn, v,
		pb.NoCompression, fs)
	require.NoError(t, err)
	sessionData := make([]byte, ssz)
	storeData := make([]byte, psz)
	_, err = rand.Read(sessionData)
	require.NoError(t, err)
	_, err = rand.Read(storeData)
	require.NoError(t, err)
	n, err := w.Write(sessionData)
	require.NoError(t, err)
	require.Equal(t, len(sessionData), n)
	m, err := w.Write(storeData)
	require.NoError(t, err)
	require.Equal(t, len(storeData), m)
	if err := w.Close(); err != nil {
		t.Fatalf("%v", err)
	}
	return w, sessionData, storeData
}

func corruptSnapshotPayload(t *testing.T, fn string, fs vfs.IFS) {
	t.Helper()
	tmpFp := fn + ".writing"
	func() {
		// upstream pebble memfs returns write-only files from ReuseForWrite,
		// rename and open the file for both read and write instead
		require.NoError(t, fs.Rename(fn, tmpFp))
		f, err := fs.OpenReadWrite(tmpFp, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, f.Close())
		}()
		s := (testSessionSize + testPayloadSize) / 2
		data := make([]byte, 1)
		_, err = f.ReadAt(data, int64(HeaderSize+s))
		require.NoError(t, err)
		data[0]++
		_, err = f.WriteAt(data, int64(HeaderSize+s))
		require.NoError(t, err)
	}()
	if err := fs.Rename(tmpFp, fn); err != nil {
		t.Fatalf("%v", err)
	}
}

func createTestSnapshotFile(t *testing.T,
	v SSVersion, fn string, fs vfs.IFS) (*SnapshotWriter, []byte, []byte) {
	t.Helper()
	return makeTestSnapshotFile(t, testSessionSize, testPayloadSize, v, fn, fs)
}

func testCorruptedPayloadWillBeDetected(t *testing.T, v SSVersion, fs vfs.IFS) {
	t.Helper()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	func() {
		createTestSnapshotFile(t, v, fp, fs)
		corruptSnapshotPayload(t, fp, fs)
		require.Panics(t, func() {
			r, header, err := NewSnapshotReader(fp, fs)
			require.NoError(t, err)
			defer func() {
				require.NoError(t, r.Close())
			}()
			_, err = rand.Read(header.PayloadChecksum)
			require.NoError(t, err)
			s := make([]byte, testSessionSize)
			p := make([]byte, testPayloadSize)
			n, err := io.ReadFull(r, s)
			require.NoError(t, err)
			require.Equal(t, testSessionSize, uint64(n))
			n, err = io.ReadFull(r, p)
			require.NoError(t, err)
			require.Equal(t, testPayloadSize, uint64(n))
			r.validatePayload()
		})
	}()
	reportLeakedFD(t, fs)
}

func TestCorruptedPayloadWillBeDetected(t *testing.T) {
	fs := vfs.GetTestFS()
	testCorruptedPayloadWillBeDetected(t, V1, fs)
	testCorruptedPayloadWillBeDetected(t, V2, fs)
}

func testNormalSnapshotCanPassValidation(t *testing.T, v SSVersion, fs vfs.IFS) {
	t.Helper()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	func() {
		_, sessionData, storeData := createTestSnapshotFile(t, v, fp, fs)
		r, _, err := NewSnapshotReader(fp, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, r.Close())
		}()
		s := make([]byte, testSessionSize)
		p := make([]byte, testPayloadSize)
		n, err := io.ReadFull(r, s)
		require.NoError(t, err)
		require.Equal(t, testSessionSize, uint64(n))
		n, err = io.ReadFull(r, p)
		require.NoError(t, err)
		require.Equal(t, testPayloadSize, uint64(n))
		require.NotPanics(t, func() {
			r.validatePayload()
		})
		require.True(t, bytes.Equal(sessionData, s))
		require.True(t, bytes.Equal(storeData, p))
	}()
	reportLeakedFD(t, fs)
}

func TestNormalSnapshotCanPassValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testNormalSnapshotCanPassValidation(t, V1, fs)
	testNormalSnapshotCanPassValidation(t, V2, fs)
}

func readTestSnapshot(fn string, sz uint64, fs vfs.IFS) ([]byte, error) {
	file, err := fs.Open(fn)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()
	data := make([]byte, sz)
	n, err := file.Read(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

func testSingleBlockSnapshotValidation(t *testing.T, sv SSVersion, fs vfs.IFS) {
	t.Helper()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	createTestSnapshotFile(t, sv, fp, fs)
	data, err := readTestSnapshot(fp, 1024*1024, fs)
	require.NoError(t, err)
	v := NewSnapshotValidator()
	require.True(t, v.AddChunk(data, 0))
	require.True(t, v.Validate())
	require.False(t, v.AddChunk(data, 0))
	// intentionally corrupt the data
	data[len(data)-1] = data[len(data)-1] + 1
	v = NewSnapshotValidator()
	require.True(t, v.AddChunk(data, 0))
	require.False(t, v.Validate())
	reportLeakedFD(t, fs)
}

func TestSingleBlockSnapshotValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testSingleBlockSnapshotValidation(t, V1, fs)
	testSingleBlockSnapshotValidation(t, V2, fs)
}

func testMultiBlockSnapshotValidation(t *testing.T, sv SSVersion, fs vfs.IFS) {
	t.Helper()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	makeTestSnapshotFile(t, 1024*1024, 1024*1024*8, sv, fp, fs)
	data, err := readTestSnapshot(fp, 1024*1024*10, fs)
	require.NoError(t, err)
	v := NewSnapshotValidator()
	c1 := data[:1024*1024]
	c2 := data[1024*1024:]
	require.True(t, v.AddChunk(c1, 0))
	require.True(t, v.AddChunk(c2, 1))
	require.True(t, v.Validate())
	v = NewSnapshotValidator()
	c2[len(c2)-1] = c2[len(c2)-1] + 1
	require.True(t, v.AddChunk(c1, 0))
	require.True(t, v.AddChunk(c2, 1))
	require.False(t, v.Validate())
	reportLeakedFD(t, fs)
}

func TestMultiBlockSnapshotValidation(t *testing.T) {
	fs := vfs.GetTestFS()
	testMultiBlockSnapshotValidation(t, V1, fs)
	testMultiBlockSnapshotValidation(t, V2, fs)
}

func TestMustInSameDir(t *testing.T) {
	fs := vfs.GetTestFS()
	tests := []struct {
		path1   string
		path2   string
		sameDir bool
	}{
		{testF1DataPath, "/d1/d2/f2.data", true},
		{testF1DataPath, "/d1/d3/f2.data", false},
		{testF1DataPath, "/d1/d2/d3/f2.data", false},
		{testF1DataPath, "f2.data", false},
		{testF1DataPath, "/d2/f2.data", false},
		{testF1DataPath, "d2/f2.data", false},
	}
	for _, tt := range tests {
		func() {
			if tt.sameDir {
				require.NotPanics(t, func() {
					mustInSameDir(tt.path1, tt.path2, fs)
				})
			} else {
				require.Panics(t, func() {
					mustInSameDir(tt.path1, tt.path2, fs)
				})
			}
		}()
	}
}

func TestShrinkSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	testDir := snapshotTestDir(t, fs)
	snapshotFilename := fs.PathJoin(testDir, "test_snapshot.data")
	shrunkFilename := fs.PathJoin(testDir, "test_snapshot.shrunk")
	writer, err := NewSnapshotWriter(snapshotFilename, pb.NoCompression, fs)
	require.NoError(t, err)
	sz := make([]byte, 8)
	binary.LittleEndian.PutUint64(sz, uint64(0))
	_, err = writer.Write(sz)
	require.NoError(t, err)
	for i := range 10 {
		data := make([]byte, 1024*1024+i*256)
		_, err := rand.Read(data)
		require.NoError(t, err)
		_, err = writer.Write(data)
		require.NoError(t, err)
	}
	err = writer.Close()
	require.NoError(t, err)
	shrunk, err := IsShrunkSnapshotFile(snapshotFilename, fs)
	require.NoError(t, err)
	require.False(t, shrunk)
	err = ShrinkSnapshot(snapshotFilename, shrunkFilename, fs)
	require.NoError(t, err)
	shrunk, err = IsShrunkSnapshotFile(snapshotFilename, fs)
	require.NoError(t, err)
	require.False(t, shrunk)
	shrunk, err = IsShrunkSnapshotFile(shrunkFilename, fs)
	require.NoError(t, err)
	require.True(t, shrunk)
	fi, err := fs.Stat(shrunkFilename)
	require.NoError(t, err)
	require.Equal(t, int64(1060), fi.Size())
	reader, _, err := NewSnapshotReader(shrunkFilename, fs)
	require.NoError(t, err)
	err = reader.Close()
	require.NoError(t, err)
	reportLeakedFD(t, fs)
}

func TestReplaceSnapshotFile(t *testing.T) {
	fs := vfs.GetTestFS()
	testDir := snapshotTestDir(t, fs)
	f1name := fs.PathJoin(testDir, "test_snapshot.data")
	f2name := fs.PathJoin(testDir, "test_snapshot.data2")
	createFile := func(fn string, sz uint64) {
		f1, err := fs.Create(fn, vfs.WriteCategoryUnspecified)
		require.NoError(t, err)
		data := make([]byte, sz)
		_, err = rand.Read(data)
		require.NoError(t, err)
		_, err = f1.Write(data)
		require.NoError(t, err)
		err = f1.Close()
		require.NoError(t, err)
	}
	createFile(f1name, 1024)
	createFile(f2name, 2048)
	err := ReplaceSnapshot(f2name, f1name, fs)
	require.NoError(t, err)
	_, err = fs.Stat(f2name)
	require.Error(t, err)
	fi, err := fs.Stat(f1name)
	require.NoError(t, err)
	require.Equal(t, int64(2048), fi.Size())
}

func testV2PayloadChecksumCanBeRead(t *testing.T, sz uint64, fs vfs.IFS) {
	t.Helper()
	fp := fs.PathJoin(snapshotTestDir(t, fs), "testsnapshot.tmp")
	func() {
		makeTestSnapshotFile(t, 0, sz, V2, fp, fs)
		reader, header, err := NewSnapshotReader(fp, fs)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, reader.Close())
		}()
		crc, err := GetV2PayloadChecksum(fp, fs)
		require.NoError(t, err)
		require.True(t, bytes.Equal(crc, header.PayloadChecksum))
	}()
	reportLeakedFD(t, fs)
}

func TestV2PayloadChecksumCanBeRead(t *testing.T) {
	fs := vfs.GetTestFS()
	testV2PayloadChecksumCanBeRead(t, blockSize-1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize+1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3-1, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3, fs)
	testV2PayloadChecksumCanBeRead(t, blockSize*3+1, fs)
}

func TestV1SnapshotCanBeLoaded(t *testing.T) {
	// rsm: idList sz 2
	// rsm: client id 15771809973567514624, responded to 4, map[5:128]
	// rsm: client id 6760681031265190231, responded to 2, map[3:128]
	fs := vfs.GetTestFS()
	if fs != vfs.DefaultFS {
		t.Skip("skipped, the fs can not access the testdata")
	}
	fp := fs.PathJoin("testdata", "v1snapshot.gbsnap")
	reader, header, err := NewSnapshotReader(fp, fs)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, reader.Close())
	}()
	require.Equal(t, uint64(1), header.Version)
	v := (SSVersion)(header.Version)
	sm := NewSessionManager()
	err = sm.LoadSessions(reader, v)
	require.NoError(t, err)
	sessions := sm.lru
	s1, ok := sessions.getSession(15771809973567514624)
	require.True(t, ok)
	s2, ok := sessions.getSession(6760681031265190231)
	require.True(t, ok)
	require.Equal(t, RaftClientID(15771809973567514624), s1.ClientID)
	require.Equal(t, RaftSeriesID(4), s1.RespondedUpTo)
	require.Equal(t, RaftClientID(6760681031265190231), s2.ClientID)
	require.Equal(t, RaftSeriesID(2), s2.RespondedUpTo)
	r1, ok := s1.History[5]
	require.True(t, ok)
	require.Equal(t, uint64(128), r1.Value)
	require.Len(t, s1.History, 1)
	r2, ok := s2.History[3]
	require.True(t, ok)
	require.Equal(t, uint64(128), r2.Value)
	require.Len(t, s2.History, 1)
	data := make([]byte, 11)
	_, err = io.ReadFull(reader, data)
	require.NoError(t, err)
	require.Equal(t, "random-data", string(data))
}

func TestValidateHeader(t *testing.T) {
	data := []byte{1, 2, 3, 4}
	require.True(t, validateHeader(data, fourZeroBytes))
	h := newCRC32Hash()
	_, err := h.Write(data)
	require.NoError(t, err)
	crc := h.Sum(nil)
	require.True(t, validateHeader(data, crc))
	crc[0]++
	require.False(t, validateHeader(data, crc))
}

func TestGetWitnessSnapshot(t *testing.T) {
	fs := vfs.GetTestFS()
	d, err := GetWitnessSnapshot(fs)
	require.NoError(t, err)
	data := GetEmptyLRUSession()
	expectedLen := HeaderSize + uint64(len(data)) + 20
	require.Equal(t, expectedLen, uint64(len(d)))
}
