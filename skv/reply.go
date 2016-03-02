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
	ReplyOK          = "OK"
	ReplyNotFound    = "NotFound"
	ReplyBadArgument = "BadArgument"
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

	ls := []Entry{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		ls = append(ls, Entry{r.Data[i], r.Data[i+1]})
	}

	return ls
}

func (r *Reply) JsonDecode(v interface{}) error {
	return JsonDecode(r.Bytes(), v)
}

func (r *Reply) Object() *Object {

	o := &Object{
		Status: r.Status,
		Meta:   ObjectMetaParse(r.Bytes()),
	}

	if o.Meta.seek > 0 {
		o.entryValue = r.Bytes()[o.Meta.seek:]
	}

	return o
}

func (r *Reply) ObjectList() []*Object {

	ls := []*Object{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {

		o := &Object{
			Meta: ObjectMetaParse(r.Data[i+1]),
			Key:  r.Data[i],
		}

		if o.Meta.seek > 0 {
			o.entryValue = r.Data[i+1][o.Meta.seek:]
		}

		ls = append(ls, o)
	}

	return ls
}

func (r *Reply) ObjectMeta() ObjectMeta {
	return ObjectMetaParse(r.Bytes())
}

func (r *Reply) ObjectMetaList() []ObjectMeta {

	ls := []ObjectMeta{}

	for i := 0; i < (len(r.Data) - 1); i += 2 {
		ls = append(ls, ObjectMetaParse(r.Data[i+1]))
	}

	return ls
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

//
type entryValue []byte

func (v entryValue) Bytes() []byte {
	return v
}

func (v entryValue) String() string {
	return string(v.Bytes())
}

func (v entryValue) Int64() int64 {

	if i64, err := strconv.ParseInt(v.String(), 10, 64); err == nil {
		return i64
	}

	return 0
}

func (v entryValue) Int32() int32 {
	return int32(v.Int64())
}

func (v entryValue) Int16() int16 {
	return int16(v.Int64())
}

func (v entryValue) Int8() int8 {
	return int8(v.Int64())
}

func (v entryValue) Int() int {
	return int(v.Int64())
}

func (v entryValue) Uint64() uint64 {

	if ui64, err := strconv.ParseUint(v.String(), 10, 64); err == nil {
		return ui64
	}

	return 0
}

func (v entryValue) Uint32() uint32 {
	return uint32(v.Uint64())
}

func (v entryValue) Uint16() uint16 {
	return uint16(v.Uint64())
}

func (v entryValue) Uint8() uint8 {
	return uint8(v.Uint64())
}

func (v entryValue) Uint() uint {
	return uint(v.Uint64())
}

func (v entryValue) Float64() float64 {

	if f64, err := strconv.ParseFloat(v.String(), 64); err == nil {
		return f64
	}

	return 0
}

func (v entryValue) Bool() bool {

	if b, err := strconv.ParseBool(v.String()); err == nil {
		return b
	}

	return false
}

func (v entryValue) JsonDecode(vi interface{}) error {
	return JsonDecode(v, vi)
}
