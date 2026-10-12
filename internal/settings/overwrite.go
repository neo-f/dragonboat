// Copyright 2017-2019 Lei Ni (nilei81@gmail.com) and other contributors.
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

package settings

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
)

func getParsedConfig(fn string) map[string]any {
	if _, err := os.Stat(fn); os.IsNotExist(err) {
		return nil
	}
	m := map[string]any{}
	b, err := os.ReadFile(filepath.Clean(fn))
	if err != nil {
		panic(err)
	}
	if err := json.Unmarshal(b, &m); err != nil {
		panic(err)
	}
	return m
}

func overwriteHardSettings(org *hard) {
	cfg := getParsedConfig("dragonboat-hard-settings.json")
	rd := reflect.Indirect(reflect.ValueOf(org))
	overwriteSettings(cfg, rd)
}

func overwriteSoftSettings(org *soft) {
	cfg := getParsedConfig("dragonboat-soft-settings.json")
	rd := reflect.Indirect(reflect.ValueOf(org))
	overwriteSettings(cfg, rd)
}

func overwriteSettings(cfg map[string]any, rd reflect.Value) {
	for key, val := range cfg {
		field := rd.FieldByName(key)
		if field.IsValid() {
			switch field.Type().String() {
			case "uint64":
				f, ok := val.(float64)
				if !ok {
					plog.Error("setting is not a float64 value",
						"setting", key)
					panic(fmt.Errorf("setting %v is not a float64 value", key))
				}
				nv := uint64(f)
				plog.Info("Setting to uint64 value", "setting", key, "value", nv)
				field.SetUint(nv)
			case "bool":
				b, ok := val.(bool)
				if !ok {
					plog.Error("setting is not a bool value",
						"setting", key)
					panic(fmt.Errorf("setting %v is not a bool value", key))
				}
				plog.Info("Setting to bool value", "setting", key, "value", b)
				field.SetBool(b)
			case "string":
				s, ok := val.(string)
				if !ok {
					plog.Error("setting is not a string value",
						"setting", key)
					panic(fmt.Errorf("setting %v is not a string value", key))
				}
				plog.Info("Setting to string value", "setting", key, "value", s)
				field.SetString(s)
			default:
			}
		}
	}
}
