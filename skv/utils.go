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
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

const (
	MetaTimeStd = "20060102150405.000"
)

func TimeNowMS() uint64 {
	return uint64(time.Now().UTC().UnixNano() / 1e6)
}

func MetaTimeNow() uint64 {

	t := time.Now().UTC().Format(MetaTimeStd)

	if u64, err := strconv.ParseUint(t[:14]+t[15:], 10, 64); err == nil {
		return u64
	}

	return 0
}

func BytesClone(src []byte) []byte {

	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

func JsonDecode(src []byte, js interface{}) (err error) {

	defer func() {
		if r := recover(); r != nil {
			err = errors.New("json: invalid format")
		}
	}()

	d := json.NewDecoder(bytes.NewBuffer(src))
	d.UseNumber()

	return d.Decode(&js)
}

func JsonEncode(js interface{}) ([]byte, error) {
	return json.Marshal(js)
}
