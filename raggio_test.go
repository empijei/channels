// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raggio_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"

	. "github.com/empijei/raggio"
)

// TODO(clap): use this instead of cmp.Diff

func cmpDiff[T any](a, b T, opts ...cmp.Option) string {
	return cmp.Diff(a, b, opts...)
}

func TestFromFunc(t *testing.T) {
	var tests = []struct {
		name string
		in   func(i int) (int, bool)
		want []int
	}{
		{
			name: "empty",
			in: func(i int) (int, bool) {
				return 0, false
			},
			want: nil,
		},
		{
			name: "ten ones",
			in: func(i int) (int, bool) {
				if i < 10 {
					return 1, true
				}
				return 0, false
			},
			want: []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name: "five entries",
			in: func(i int) (int, bool) {
				if i < 5 {
					return i, true
				}
				return 0, false
			},
			want: []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := FromFunc(tt.in)()
			var got []int
			for v := range ch {
				got = append(got, v)
			}
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestToSlice(t *testing.T) {
	var tests = []struct {
		name string
		s    []int
	}{
		{
			name: "empty",
		},
		{
			name: "ten ones",
			s:    []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name: "five entries",
			s:    []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := make(chan int, len(tt.s))
			for _, v := range tt.s {
				ch <- v
			}
			close(ch)
			got := ToSlice(ch)
			if diff := cmpDiff(tt.s, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestToSlices(t *testing.T) {
	var tests = []struct {
		name string
		a, b []int
	}{
		{
			name: "empty",
		},
		{
			name: "five",
			a:    []int{1, 2, 3, 4},
			b:    []int{5, 6, 7, 8},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a, b := FromSlice(tt.a)(), FromSlice(tt.b)()
			gota, gotb := ToSlices(a, b)
			if diff := cmpDiff(tt.a, gota); diff != "" {
				t.Errorf("slice a: -want +got:\n%s", diff)
			}
			if diff := cmpDiff(tt.b, gotb); diff != "" {
				t.Errorf("slice b: -want +got:\n%s", diff)
			}
		})
	}
}

func TestFromSlice(t *testing.T) {
	var tests = []struct {
		name string
		s    []int
	}{
		{
			name: "empty",
		},
		{
			name: "ten ones",
			s:    []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1},
		},
		{
			name: "five entries",
			s:    []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := FromSlice(tt.s)()
			var got []int
			for v := range ch {
				got = append(got, v)
			}
			if diff := cmpDiff(tt.s, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

// TODO TestFromTicker

func TestFromRange(t *testing.T) {
	var tests = []struct {
		name string
		s, e int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "zero start",
			s:    0,
			e:    4,
			want: []int{0, 1, 2, 3},
		},
		{
			name: "five entries",
			s:    4,
			e:    8,
			want: []int{4, 5, 6, 7},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := FromRange(tt.s, tt.e)()
			got := ToSlice(ch)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

// TODO CombineLatest

func TestConcat(t *testing.T) {
	var tests = []struct {
		name string
		in   [][]int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "three",
			in:   [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var chs []<-chan int
			for _, s := range tt.in {
				chs = append(chs, FromSlice(s)())
			}
			ch := Concat[int]()(FromSlice(chs)())
			got := ToSlice(ch)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

// TODO ForkJoin

func TestMerge(t *testing.T) {

	// TODO: Test that we actually emit them in a meaningful order

	var tests = []struct {
		name string
		in   [][]int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "three",
			in:   [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}},
			want: []int{1, 2, 3, 4, 5, 6, 7, 8, 9},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var chs []<-chan int
			for _, s := range tt.in {
				chs = append(chs, FromSlice(s)())
			}
			ch := Merge[int]()(FromSlice(chs)())
			got := ToSlice(ch)
			slices.Sort(got)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func mkSlice(s, e int) []int {
	var r []int
	for i := s; i < e; i++ {
		r = append(r, i)
	}
	return r
}

func TestPartition(t *testing.T) {
	var tests = []struct {
		name  string
		in    []int
		f     func(i int) bool
		wantT []int
		wantE []int
	}{
		{
			name: "empty",
			f:    func(i int) bool { return i%3 == 0 },
		},
		{
			name:  "div three",
			in:    mkSlice(0, 10),
			f:     func(i int) bool { return i%3 == 0 },
			wantT: []int{0, 3, 6, 9},
			wantE: []int{1, 2, 4, 5, 7, 8},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := FromSlice(tt.in)()
			a, b := Partition(tt.f)(ch)
			gota, gotb := ToSlices(a, b)
			if diff := cmpDiff(tt.wantT, gota); diff != "" {
				t.Errorf("Partition: then: -want +got:\n%s", diff)
			}
			if diff := cmpDiff(tt.wantE, gotb); diff != "" {
				t.Errorf("Partition: else: -want +got:\n%s", diff)
			}
		})
	}
}
