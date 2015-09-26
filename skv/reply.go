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
	"strconv"
)

const (
	ReplyOK              = "OK"
	ReplyNotFound        = "NotFound"
	ReplyInvalidArgument = "InvalidArgument"
)

type Reply struct {
	Status string
	Data   [][]byte
}

type Entry struct {
	Key, Value []byte
}

func NewReply(status string) *Reply {

	if status == "" {
		status = ReplyOK
	}

	return &Reply{
		Status: status,
	}
}

func (r *Reply) Bytes() []byte {

	if len(r.Data) > 0 {
		return r.Data[0]
	}

	return []byte{}
}

func (r *Reply) String() string {
	return string(r.Bytes())
}

func (r *Reply) Int64() int64 {

	if i64, err := strconv.ParseInt(r.String(), 10, 64); err == nil {
		return i64
	}

	return 0
}

func (r *Reply) Int() int {
	return int(r.Int64())
}

func (r *Reply) Int32() int32 {
	return int32(r.Int64())
}

func (r *Reply) Int16() int16 {
	return int16(r.Int64())
}

func (r *Reply) Int8() int8 {
	return int8(r.Int64())
}

func (r *Reply) Uint64() uint64 {

	if ui64, err := strconv.ParseUint(r.String(), 10, 64); err == nil {
		return ui64
	}

	return 0
}

func (r *Reply) Uint() uint {
	return uint(r.Uint64())
}

func (r *Reply) Uint32() uint32 {
	return uint32(r.Uint64())
}

func (r *Reply) Uint16() uint16 {
	return uint16(r.Uint64())
}

func (r *Reply) Uint8() uint8 {
	return uint8(r.Uint64())
}

func (r *Reply) Float64() float64 {

	if f64, err := strconv.ParseFloat(r.String(), 64); err == nil {
		return f64
	}

	return 0
}

func (r *Reply) Bool() bool {

	if b, err := strconv.ParseBool(r.String()); err == nil {
		return b
	}

	return false
}

func (r *Reply) List() [][]byte {
	return r.Data
}

func (r *Reply) Hash() []Entry {

	hs := []Entry{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		hs = append(hs, Entry{r.Data[i], r.Data[i+1]})
	}

	return hs
}

func (r *Reply) JsonDecode(v interface{}) error {
	return JsonDecode(r.Bytes(), v)
}

func (e *Entry) String() string {
	return string(e.Value)
}

func (e *Entry) Uint64() uint64 {

	if ui64, err := strconv.ParseUint(e.String(), 10, 64); err == nil {
		return ui64
	}

	return 0
}

func (e *Entry) JsonDecode(v interface{}) error {
	return JsonDecode(e.Value, v)
}
