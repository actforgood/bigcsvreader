// Copyright 2022 Bogdan Constantinescu.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file or at
// https://github.com/actforgood/bigcsvreader/blob/main/LICENSE.

package internal_test

import (
	"testing"

	"github.com/actforgood/bigcsvreader/internal"
)

func init() {
	var _ internal.Logger = (*internal.NopLogger)(nil) // ensure NopLogger is a Logger
}

func TestNopLogger(t *testing.T) {
	// Note: this test is more for coverage, does not test anything after all.
	t.Parallel()

	// arrange
	subject := internal.NopLogger{}

	// act
	subject.Error("foo", "bar", "abc", 123)
	subject.Debug("foo", "bar", "abc", 123, "err", "some error")
}
