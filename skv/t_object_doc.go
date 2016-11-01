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

package skv

import (
	"encoding/binary"
	"reflect"
	"strconv"
	"strings"

	"github.com/lessos/lessdb/dbutil"
)

func ObjectDocFoldKey(fold string) []byte {
	return stringToHashBytes(dbutil.ObjectPathClean(fold), ObjectFoldLength)
}

func ObjectDocSchemaKey(key []byte) []byte {
	return RawNsKeyConcat(nsObjectDocSchema, key)
}

func ObjectDocIndexFieldPrefix(key []byte, column uint8) []byte {
	return append(RawNsKeyConcat(nsObjectDocIndex, key), column)
}

func ObjectDocIndexIncrKey(key []byte, column uint8) []byte {
	return append(RawNsKeyConcat(nsObjectDocIncrement, key), column)
}

func ObjectDocBytesIncr(key []byte) []byte {

	for i := (len(key) - 1); i >= 0; i-- {

		if key[i] == 255 {
			key[i] = 0
		} else {
			key[i] = key[i] + 1
			break
		}
	}

	return key
}

func ObjectDocBytesDecr(key []byte) []byte {

	for i := (len(key) - 1); i >= 0; i-- {

		if key[i] == 0 {
			key[i] = 255
		} else {
			key[i] = key[i] - 1
			break
		}
	}

	return key
}

func ObjectDocIndexRawKeyExport(data []byte, ilen uint8) ([]byte, []byte, bool) {

	if len(data) < 7 {
		return []byte{}, []byte{}, false
	}

	kplen := int(data[1] + 3)

	pken := kplen + int(ilen)

	if len(data) <= pken {
		return []byte{}, []byte{}, false
	}

	return data[kplen:pken], data[pken:], true
}

func ObjectDocIndexStringFilter(key string) string {
	return strings.ToLower(strings.TrimSpace(key))
}

func ObjectDocIndexValue(idx *ObjectDocSchemaIndexEntry, value reflect.Value) ([]byte, bool) {

	ui64, bs := uint64(0), []byte{}

	if value.Kind() == reflect.Interface {
		value = reflect.ValueOf(value.Interface())
	}

	switch value.Kind() {

	// []byte
	case reflect.Slice:
		bs = value.Bytes()

	// uint*
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		ui64 = value.Uint()

	// int*
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if i64 := value.Int(); i64 >= 0 {
			ui64 = uint64(i64)
		}

	// float*
	case reflect.Float32, reflect.Float64:
		if f64 := value.Float(); f64 >= 0 {
			ui64 = uint64(f64)
		}

	// bool
	case reflect.Bool:
		if value.Bool() {
			ui64 = 1
		}

	// string
	case reflect.String:
		if idx.Type == ObjectDocSchemaIndexTypeUint {
			ui64, _ = strconv.ParseUint(value.String(), 10, 64)
		} else {
			bs = stringToHashBytes(ObjectDocIndexStringFilter(value.String()), 20)
		}

	//
	default:
		return bs, false
	}

	//
	switch idx.Type {

	case ObjectDocSchemaIndexTypeUint:

		ibs := make([]byte, 8)
		binary.BigEndian.PutUint64(ibs, ui64)

		if idx.Length > 8 {
			idx.Length = 8
		} else if idx.Length < 1 {
			idx.Length = 1
		}

		bs = ibs[(8 - idx.Length):]

	case ObjectDocSchemaIndexTypeString, ObjectDocSchemaIndexTypeBinary:

		if idx.Length > 16 {
			idx.Length = 16
		} else if idx.Length < 1 {
			idx.Length = 1
		}

		if len(bs) > int(idx.Length) {
			bs = bs[:idx.Length]
		}

	default:
		return bs, false
	}

	return bs, (len(bs) == int(idx.Length))
}
