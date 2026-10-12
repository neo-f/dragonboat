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

package registry

import (
	"context"
	"log"
	"log/slog"
	"strings"
)

type gossipLogWriter struct {
	logger *slog.Logger
}

func (l *gossipLogWriter) Write(p []byte) (int, error) {
	str := string(p)
	str = strings.TrimSuffix(str, "\n")
	ctx := context.Background()

	if s, found := strings.CutPrefix(str, "[WARN] "); found {
		l.logger.Log(ctx, slog.LevelWarn, s)
	} else if s, found := strings.CutPrefix(str, "[DEBUG] "); found {
		l.logger.Log(ctx, slog.LevelDebug, s)
	} else if s, found := strings.CutPrefix(str, "[INFO] "); found {
		l.logger.Log(ctx, slog.LevelInfo, s)
	} else if s, found := strings.CutPrefix(str, "[ERR] "); found {
		l.logger.Log(ctx, slog.LevelWarn, s)
	} else {
		l.logger.Log(ctx, slog.LevelWarn, str)
	}

	return len(p), nil
}

// newGossipLogWrapper prepare log wrapper for gossip.
// Inspirited by https://github.com/docker/docker-ce/blob/master/components/engine/libnetwork/networkdb/shard.go#L30
func newGossipLogWrapper() *log.Logger {
	return log.New(&gossipLogWriter{
		logger: slog.With("pkg", "gossip"),
	}, "", 0)
}
