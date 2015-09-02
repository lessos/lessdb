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

package leveldb

const (
	ttl_job_sleep            = 300e6
	ttl_job_limit     uint64 = 10000
	scan_max_limit    uint64 = 10000
	ns_zero                  = 0x00
	ns_set_entry             = 0x80
	ns_hash_entry            = 0x81
	ns_hash_len              = 0x82
	ns_zset_entry            = 0x83
	ns_zset_score            = 0x84
	ns_zset_length           = 0x85
	ns_iset_schema           = 0x86
	ns_iset_entry            = 0x87
	ns_iset_index            = 0x88
	ns_iset_length           = 0x89
	ns_iset_increment        = 0x90
)

var (
	ns_set_ttl = []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00}
)
