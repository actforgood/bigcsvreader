// Copyright The ActForGood Authors.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

// Package bigcsvreader offers a multi-threaded approach for reading a large CSV file
// in order to improve the time of reading and processing it.
// It spawns multiple goroutines, each reading a piece of the file.
// Read rows are put into channels equal in number to the spawned goroutines,
// in this way also the processing of those rows can be parallelized.
package bigcsvreader
