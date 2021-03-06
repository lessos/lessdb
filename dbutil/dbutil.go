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
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
)

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

func ObjectPathClean(path string) string {
	return strings.Trim(strings.Trim(filepath.Clean(path), "/"), ".")
}

func BytesClone(src []byte) []byte {

	dst := make([]byte, len(src))
	copy(dst, src)

	return dst
}

func BytesConcat(vs ...[]byte) []byte {

	var bs []byte

	for _, v := range vs {
		bs = append(bs, v...)
	}

	return bs
}

func RandomHexString(length int) string {

	if length < 1 {
		length = 1
	} else if length > 16 {
		length = 16
	}

	key := make([]byte, length)

	io.ReadFull(rand.Reader, key)

	return hex.EncodeToString(key)
}

func BytesToHexString(key []byte) string {
	return hex.EncodeToString(key)
}

func HexStringToBytes(key string) []byte {

	if v, err := hex.DecodeString(key); err == nil {
		return v
	}

	return []byte{}
}

func BytesToUint64(key []byte) uint64 {

	if len(key) < 1 || len(key) > 8 {
		return 0
	}

	uibs := make([]byte, 8)

	offset := 8 - len(key)

	for i := 0; i < len(key); i++ {
		uibs[i+offset] = key[i]
	}

	return binary.BigEndian.Uint64(uibs)
}

func BytesToUint32(key []byte) uint32 {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("\tBytesToUint32", r)
		}
	}()

	if len(key) < 1 || len(key) > 4 {
		return 0
	}

	uibs := make([]byte, 4)

	offset := 4 - len(key)

	for i := 0; i < 4; i++ {
		uibs[i+offset] = key[i]
	}

	return binary.BigEndian.Uint32(uibs)
}

func SintToBytes(sint string, lg uint8) []byte {

	if lg < 1 {
		lg = 1
	} else if lg > 8 {
		lg = 8
	}

	ui64, _ := strconv.ParseUint(sint, 10, 64)

	uibs := make([]byte, 8)

	binary.BigEndian.PutUint64(uibs, ui64)

	return uibs[8-lg:]
}

func Uint64ToBytes(v uint64) []byte {

	bs := make([]byte, 8)
	binary.BigEndian.PutUint64(bs, v)

	return bs
}

func Uint32ToBytes(v uint32) []byte {

	defer func() {
		if r := recover(); r != nil {
			fmt.Println("\tBytesToUint32", r)
		}
	}()

	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, v)

	return bs
}
