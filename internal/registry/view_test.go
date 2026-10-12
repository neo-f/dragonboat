// Copyright 2017-2022 Lei Ni (nilei81@gmail.com) and other contributors.
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

package registry

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/neo-f/dragonboat/v4/internal/raft"
)

func getTestShardView() []ShardView {
	cv1 := ShardView{
		ShardID:           100,
		ConfigChangeIndex: 200,
		Replicas: map[uint64]string{
			200: "address1",
			300: "address2",
			400: "address3",
		},
	}
	cv2 := ShardView{
		ShardID:           1340,
		ConfigChangeIndex: 126200,
		Replicas: map[uint64]string{
			1200: "myaddress1",
			4300: "theiraddress2",
			6400: "heraddress3",
		},
	}
	return []ShardView{cv1, cv2}
}

func TestGetFullSyncData(t *testing.T) {
	v := newView(123)
	cv := getTestShardView()
	v.update(cv)
	data := v.getFullSyncData()

	v2 := newView(123)
	v2.updateFrom(data)
	assert.Equal(t, v, v2)
}

func TestConfigChangeIndexIsChecked(t *testing.T) {
	v := newView(123)
	cv := getTestShardView()
	v.update(cv)

	update := []ShardView{
		{
			ShardID:           1340,
			ConfigChangeIndex: 300,
			Replicas: map[uint64]string{
				1200: "myaddress1",
				4300: "theiraddress2",
			},
		},
	}
	v.update(update)
	c, ok := v.mu.shards[1340]
	assert.True(t, ok)
	assert.Equal(t, uint64(126200), c.ConfigChangeIndex)
	assert.Len(t, c.Replicas, 3)

	update = []ShardView{
		{
			ShardID:           1340,
			ConfigChangeIndex: 226200,
			Replicas: map[uint64]string{
				1200: "myaddress1",
				4300: "theiraddress2",
			},
		},
	}
	v.update(update)
	c, ok = v.mu.shards[1340]
	assert.True(t, ok)
	assert.Equal(t, uint64(226200), c.ConfigChangeIndex)
	assert.Len(t, c.Replicas, 2)
}

func TestDeploymentIDIsChecked(t *testing.T) {
	v := newView(123)
	cv := getTestShardView()
	v.update(cv)
	data := v.getFullSyncData()

	v2 := newView(321)
	v2.updateFrom(data)
	assert.Empty(t, v2.mu.shards)
}

func TestGetGossipData(t *testing.T) {
	v := newView(123)
	cv := getTestShardView()
	v.update(cv)
	data := v.getGossipData(340)
	assert.NotEmpty(t, data)
}

func runMembershipUpdateTest(t *testing.T, updateIndex uint64, expectUpdated bool) {
	t.Helper()
	v := newView(0)
	cv := ShardView{
		ShardID:           123,
		ConfigChangeIndex: 100,
		Replicas:          make(map[uint64]string),
	}
	cv.Replicas[1] = "t1"
	cv.Replicas[2] = "t2"
	v.mu.shards[123] = cv

	ncv := ShardView{
		ShardID:           123,
		ConfigChangeIndex: updateIndex,
		Replicas:          make(map[uint64]string),
	}
	ncv.Replicas[1] = "t1"
	ncv.Replicas[2] = "t2"
	ncv.Replicas[3] = "t3"
	updates := []ShardView{ncv}
	v.update(updates)

	result, ok := v.mu.shards[123]
	assert.True(t, ok)
	if expectUpdated {
		assert.Equal(t, ncv, result)
	} else {
		assert.Equal(t, cv, result)
	}
}

func TestUpdateMembershipView(t *testing.T) {
	runMembershipUpdateTest(t, 200, true)
}

func TestOutOfDateMembershipInfoIsIgnored(t *testing.T) {
	runMembershipUpdateTest(t, 10, false)
}

func TestUpdateLeadershipView(t *testing.T) {
	v := newView(0)
	cv := ShardView{
		ShardID:  123,
		LeaderID: 10,
		Term:     20,
	}
	v.mu.shards[123] = cv

	ncv := ShardView{
		ShardID:  123,
		LeaderID: 11,
		Term:     21,
	}
	updates := []ShardView{ncv}
	v.update(updates)

	result, ok := v.mu.shards[123]
	assert.True(t, ok)
	assert.Equal(t, ncv, result)
}

func TestInitialLeaderInfoCanBeRecorded(t *testing.T) {
	v := newView(0)
	cv := ShardView{
		ShardID: 123,
	}
	v.mu.shards[123] = cv

	ncv := ShardView{
		ShardID:  123,
		LeaderID: 11,
		Term:     21,
	}
	updates := []ShardView{ncv}
	v.update(updates)

	result, ok := v.mu.shards[123]
	assert.True(t, ok)
	assert.Equal(t, ncv, result)
}

func TestUnknownLeaderIsIgnored(t *testing.T) {
	v := newView(0)
	cv := ShardView{
		ShardID:  123,
		LeaderID: 10,
		Term:     20,
	}
	v.mu.shards[123] = cv

	ncv := ShardView{
		ShardID:  123,
		LeaderID: raft.NoLeader,
		Term:     21,
	}
	updates := []ShardView{ncv}
	v.update(updates)

	result, ok := v.mu.shards[123]
	assert.True(t, ok)
	assert.Equal(t, cv, result)
}
