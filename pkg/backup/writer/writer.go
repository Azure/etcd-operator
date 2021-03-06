// Copyright 2017 The etcd-operator Authors
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

package writer

import "io"

// Writer defines the required writer operations.
type Writer interface {
	// Write writes a backup file to the given path and returns size of written file.
	Write(path string, r io.Reader) (int64, error)
	// Purge purges stale backup files according to the appended revision number
	Purge(path string, maxBackups int) error
}
