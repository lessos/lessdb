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

import (
	"encoding/binary"
)

func SortSetsKey(key, member []byte) []byte {
	return append(RawKeyEncode(ns_ss_entry, key), member...)
}

func SortSetsLenKey(key []byte) []byte {
	return RawKeyEncode(ns_ss_length, key)
}

func SortSetsScoreKeyPrefix(key []byte, score uint64) []byte {

	bscore := make([]byte, 8)
	binary.BigEndian.PutUint64(bscore, score)

	return append(RawKeyEncode(ns_ss_score, key), bscore...)
}

func SortSetsScoreKey(key, member []byte, score uint64) []byte {
	return append(SortSetsScoreKeyPrefix(key, score), member...)
}
