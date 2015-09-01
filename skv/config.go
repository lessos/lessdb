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
	Driver              string `json:"driver,omitempty"`
	DataDir             string `json:"datadir,omitempty"`
	CacheCapacity       int    `json:"cache_capacity,omitempty"`
	CompactionTableSize int
	BlockCacheCapacity  int
	WriteBuffer         int
}

var (
	DefaultConfig = Config{
		Driver:              "goleveldb",
		DataDir:             "./var",
		CacheCapacity:       8,
		CompactionTableSize: 2, // 16 // up-default 2 MiB
		BlockCacheCapacity:  8, // 64, // up-default 8 MiB
		WriteBuffer:         4, // 4 MiB
	}
)
