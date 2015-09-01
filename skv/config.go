// Copyright 2015 lessOS.com, All rights reserved.
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

package skv

type Config struct {
	Driver                 string `json:"driver,omitempty"`
	DataDir                string `json:"datadir,omitempty"`
	WriteBuffer            int    `json:"write_buffer,omitempty"`
	BlockCacheCapacity     int    `json:"block_cache_capacity,omitempty"`
	CacheCapacity          int    `json:"cache_capacity,omitempty"`
	OpenFilesCacheCapacity int    `json:"open_files_cache_capacity,omitempty"`
	CompactionTableSize    int    `json:"compaction_table_size,omitempty"`
}

var (
	DefaultConfig = Config{
		Driver:                 "goleveldb",
		DataDir:                "./var",
		WriteBuffer:            4,   // 4 MiB
		CacheCapacity:          8,   // 8 MiB
		OpenFilesCacheCapacity: 500, // 500
		BlockCacheCapacity:     8,   // 8 MiB
		CompactionTableSize:    2,   // 2 MiB
	}
)

func (cfg *Config) ReFix() {

	if cfg.WriteBuffer < 4 {
		cfg.WriteBuffer = 4
	} else if cfg.WriteBuffer > 128 {
		cfg.WriteBuffer = 128
	}

	if cfg.CacheCapacity < 8 {
		cfg.CacheCapacity = 8
	} else if cfg.CacheCapacity > 4096 {
		cfg.CacheCapacity = 4096
	}

	if cfg.BlockCacheCapacity < 2 {
		cfg.BlockCacheCapacity = 2
	} else if cfg.BlockCacheCapacity > 32 {
		cfg.BlockCacheCapacity = 32
	}

	if cfg.OpenFilesCacheCapacity < 500 {
		cfg.OpenFilesCacheCapacity = 500
	} else if cfg.OpenFilesCacheCapacity > 30000 {
		cfg.OpenFilesCacheCapacity = 30000
	}

	if cfg.CompactionTableSize < 2 {
		cfg.CompactionTableSize = 2
	} else if cfg.CompactionTableSize > 128 {
		cfg.CompactionTableSize = 128
	}
}
