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

package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"

	"github.com/lessos/lessdb/skv"
	skvdrv "github.com/lessos/lessdb/skv/goleveldb"
)

var (
	arg_leveldb_path_base = ""
	arg_leveldb_path_comp = ""
	arg_msg               = `Usage:
  ./leveldb-diff <base leveldb folder> <compared leveldb folder>
`
)

func main() {

	flag.Parse()

	if len(os.Args) < 3 {
		fmt.Println("invalid args")
		fmt.Println(arg_msg)
		return
	}

	arg_leveldb_path_base = os.Args[1]
	arg_leveldb_path_comp = os.Args[2]

	var (
		status_num       int64  = 0
		status_hit_key   int64  = 0
		status_hit_value int64  = 0
		limit            uint32 = 10000
		offset                  = []byte{2}
	)

	skvc_base, err := skvdrv.Open(skv.Config{
		DataDir:      arg_leveldb_path_base,
		DataDirGroup: "",
	})
	if err != nil {
		fmt.Println("skvdrv.Open", err)
		return
	}
	defer skvc_base.Close()

	skvc_comp, err := skvdrv.Open(skv.Config{
		DataDir:      arg_leveldb_path_comp,
		DataDirGroup: "",
	})
	if err != nil {
		fmt.Println("skvdrv.Open", err)
		return
	}
	defer skvc_comp.Close()

	for {

		ls := skvc_base.RawScan(offset, []byte{255}, limit).Hash()
		for _, v := range ls {

			offset = v.Key

			if rs := skvc_comp.RawGet(v.Key); rs.Status == "OK" {

				status_hit_key++

				if bytes.Compare(v.Value, rs.Bytes()) == 0 {
					status_hit_value++
				}
			}
		}

		status_num += int64(len(ls))

		if uint32(len(ls)) < limit {
			break
		}

		if status_num > 0 {
			fmt.Printf("base %d, hit keys %d %.8f, hit values: %d %.8f\r",
				status_num,
				status_hit_key, float64(status_num-status_hit_key)/float64(status_num),
				status_hit_value, float64(status_num-status_hit_value)/float64(status_num))
		}
	}

	if status_num > 0 {
		fmt.Printf("base %d, hit keys %d %.8f, hit values: %d %.8f\n",
			status_num,
			status_hit_key, float64(status_num-status_hit_key)/float64(status_num),
			status_hit_value, float64(status_num-status_hit_value)/float64(status_num))
	}
}
