/*
Copyright 2024 The Numaproj Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// MustHash returns a sha256 encoded string based on the given argument.
func MustHash(v any) string {
	switch data := v.(type) {
	case []byte:
		hash := sha256.New()
		if _, err := hash.Write(data); err != nil {
			panic(err)
		}
		return hex.EncodeToString(hash.Sum(nil))
	case string:
		return MustHash([]byte(data))
	default:
		return MustHash([]byte(MustJSON(v)))
	}
}

// MustJSON makes sure the in argument is a valid JSON struct
// and returns its marshalled string version.
func MustJSON(in any) string {
	if data, err := json.Marshal(in); err != nil {
		panic(err)
	} else {
		return string(data)
	}
}
