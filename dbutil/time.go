// Copyright 2015-2016 lessdb Author, All rights reserved.
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

package dbutil

import (
	"strconv"
	"time"
)

const (
	meta_time_std     = "20060102150405.000"
	meta_time_std_sec = "20060102150405"
)

func TimeNowMS() uint64 {
	return uint64(time.Now().UTC().UnixNano() / 1e6)
}

func MetaTimeNow() uint64 {

	t := time.Now().UTC().Format(meta_time_std)

	if u64, err := strconv.ParseUint(t[:14]+t[15:], 10, 64); err == nil {
		return u64
	}

	return 0
}

func MetaTimeNowAddMS(add int64) uint64 {

	t := time.Now().UTC().Add(time.Duration(add * 1e6)).Format(meta_time_std)

	if u64, err := strconv.ParseUint(t[:14]+t[15:], 10, 64); err == nil {
		return u64
	}

	return 0
}

func MetaTimeFormat(t uint64, fm string) string {

	if fm == "rfc3339" {
		fm = time.RFC3339
	}

	return MetaTimeParse(t).Local().Format(fm)
}

func MetaTimeParse(t uint64) time.Time {

	tp, err := time.ParseInLocation(meta_time_std_sec, strconv.FormatUint(t/1000, 10), time.UTC)
	if err != nil {
		tp = time.Now()
	}

	return tp
}
