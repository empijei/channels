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
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"

	. "github.com/empijei/raggio"
)

// TODO(clap): use this instead of cmp.Diff

func cmpDiff[T any](a, b T, opts ...cmp.Option) string {
	return cmp.Diff(a, b, opts...)
}

func mkSlice(s, e int) []int {
	var r []int
	for i := s; i < e; i++ {
		r = append(r, i)
	}
	return r
}

func flush() {
	// TODO: this is bad, find a way to not need this.
	for i := 0; i < runtime.NumCPU()*runtime.NumGoroutine(); i++ {
		runtime.Gosched()
	}
	time.Sleep(1 * time.Millisecond)
}

type stubTicker chan time.Time

func newStubTickerFactory(t *testing.T, wantDuration time.Duration, s stubTicker) func(time.Duration) Ticker {
	return func(d time.Duration) Ticker {
		t.Helper()
		if wantDuration != 0 && wantDuration != d {
			t.Errorf("TickerFactory: got duration %v want %v", d, wantDuration)
		}
		return s
	}
}
func newStubTicker() stubTicker             { return make(chan time.Time) }
func (stubTicker) Stop()                    {}
func (s stubTicker) Chan() <-chan time.Time { return s }

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

func TestToSliceParallel(t *testing.T) {
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
			got := <-ToSliceParallel(ch)
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

func TestRace(t *testing.T) {

	var (
		cna, cnb, cnc bool
	)
	ca := func() { cna = true }
	cb := func() { cnb = true }
	cc := func() { cnc = true }

	a, b, c := make(chan string), make(chan string), make(chan string)

	ch := Race[string](ca, cb, cc)(a, b, c)
	var got []string

	b <- "b" // 1: B wins the race
	got = append(got, <-ch)
	c <- "c" // Discarded
	a <- "a" // Discarded
	b <- "b" // 2
	got = append(got, <-ch)
	b <- "b" // 3
	got = append(got, <-ch)
	a <- "a" // Discarded
	c <- "c" // Discarded
	c <- "c" // Discarded
	c <- "c" // Discarded
	close(a)
	close(b)
	close(c)
	for v := range ch {
		got = append(got, v)
	}

	if cna == false || cnc == false {
		t.Errorf("Race didn't cancel the ones that lost the race")
	}

	if cnb {
		t.Errorf("Race cancelled the winner")
	}

	if diff := cmpDiff([]string{"b", "b", "b"}, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestZip(t *testing.T) {
	var tests = []struct {
		name string
		a, b []int

		mustCancelA bool
		mustCancelB bool
		want        []Pair[int, int]
	}{
		{
			name: "empty",
		},
		{
			name: "same length",
			a:    []int{1, 2, 3},
			b:    []int{2, 4, 6},

			want: []Pair[int, int]{{1, 2}, {2, 4}, {3, 6}},
		},
		{
			name:        "different length",
			a:           []int{1, 2, 3, 4, 5},
			b:           []int{2, 4, 6},
			mustCancelA: true,
			want:        []Pair[int, int]{{1, 2}, {2, 4}, {3, 6}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			var ca, cb bool
			fca := func() { ca = true }
			fcb := func() { cb = true }

			a, b := FromSlice(tt.a)(), FromSlice(tt.b)()
			gotc := Zip[int, int](fca, fcb)(a, b)
			got := ToSlice(gotc)

			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}

			if tt.mustCancelA && !ca {
				t.Errorf("Should have cancelled A, but didn't")
			}

			if tt.mustCancelB && !cb {
				t.Errorf("Should have cancelled B, but didn't")
			}

		})
	}
}

func TestBuffer(t *testing.T) {
	emitch := make(chan struct{})
	emit := func() { emitch <- struct{}{} }
	buffered := Buffer[int](emitch)
	in := make(chan int)
	gotch := ToSliceParallel(buffered(in))

	emit() // Attempt empty emission, should be absorbed.
	in <- 1
	in <- 2
	emit() // [1, 2]
	emit() // Attempt empty emission, should be absorbed.
	in <- 1
	emit() // [1]
	in <- 1
	in <- 2
	in <- 3
	emit() // [1,2,3]
	in <- 1
	close(in) // [1] Leftorvers should be emitted

	got := <-gotch

	want := [][]int{{1, 2}, {1}, {1, 2, 3}, {1}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}

}

func TestBufferCount(t *testing.T) {
	var tests = []struct {
		name string
		c    int
		in   []int
		want [][]int
	}{
		{
			name: "empty",
		},
		{
			name: "exact count",
			c:    2,
			in:   []int{1, 2, 3, 4, 5, 6},
			want: [][]int{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name: "leftovers 1",
			c:    2,
			in:   []int{1, 2, 3, 4, 5, 6, 7},
			want: [][]int{{1, 2}, {3, 4}, {5, 6}, {7}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			gotch := BufferCount[int](tt.c)(in)
			got := ToSlice(gotch)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestBufferTime(t *testing.T) {
	s := newStubTicker()
	f := newStubTickerFactory(t, 1*time.Second, s)
	now := time.Now()
	emit := func() {
		now = now.Add(1 * time.Second)
		s <- now
	}
	buffered := BufferTime[int](f, 1*time.Second)
	in := make(chan int)
	gotch := ToSliceParallel(buffered(in))

	emit() // Attempt empty emission, should be absorbed.
	in <- 1
	in <- 2
	emit() // [1, 2]
	emit() // Attempt empty emission, should be absorbed.
	in <- 3
	emit() // [3]
	in <- 4
	in <- 5
	in <- 6
	emit() // [4,5,6]
	in <- 7
	close(in) // [1] Leftorvers should be emitted

	got := <-gotch

	want := [][]int{{1, 2}, {3}, {4, 5, 6}, {7}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestBufferToggle(t *testing.T) {
	openings := make(chan struct{})
	open := func() { openings <- struct{}{} }
	closings := make(chan struct{})
	clos := func() { closings <- struct{}{} }

	buffered := BufferToggle[int](openings, closings)
	in := make(chan int)
	gotch := ToSliceParallel(buffered(in))

	in <- 1 // Discarded
	in <- 2 // Discarded
	open()
	in <- 3
	in <- 4
	clos()  // [3, 4]
	in <- 5 // Discarded
	open()
	in <- 6
	clos() // [6]
	open()
	in <- 7
	close(in) // [7] Leftorvers should be emitted

	got := <-gotch

	want := [][]int{{3, 4}, {6}, {7}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

// TODO: TestConcatMap

func TestExhaustMap(t *testing.T) {
	inners := make(chan (chan string))
	var mu sync.Mutex
	var calls []int

	project := func(i int) <-chan string {
		mu.Lock()
		calls = append(calls, i)
		mu.Unlock()
		return <-inners
	}

	in := make(chan int)
	mapped := ExhaustMap(project)
	gotch := ToSliceParallel(mapped(in))

	{
		prj := make(chan string)
		in <- 1       // project(1)
		inners <- prj // Unblock call to project
		prj <- "a"    // "a"
		in <- 2       // Ignored
		prj <- "b"    // "b"
		close(prj)    // Re-enable in
	}
	// TODO: even with this flush, it might happen that the previous "close"
	// call might not have been picked up.
	// This means that we could observe different outputs for this test.
	// For example an output that is *just* the first case is acceptable, as
	// the following cases might all be discarded if the "close" in the previous
	// step gets picked up very late.
	// We need to figure out a way to deflake this and every test that uses flush.
	flush()
	{
		prj := make(chan string)
		in <- 3       // project(3)
		inners <- prj // Unblock call to project
		close(prj)    // Re-enable in
	}
	flush()
	{
		prj := make(chan string)
		in <- 4       // project(4)
		inners <- prj // Unblock call to project
		prj <- "c"    // "a"
		prj <- "d"    // "b"
		close(prj)    // Re-enable in
	}
	flush()
	close(in)

	got := <-gotch
	want := []string{"a", "b", "c", "d"}

	mu.Lock()
	defer mu.Unlock()
	if diff := cmpDiff([]int{1, 3, 4}, calls); diff != "" {
		t.Errorf("project calls: -want +got:\n%s", diff)
	}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("values: -want +got:\n%s", diff)
	}
}

func TestMap(t *testing.T) {
	var tests = []struct {
		name string
		in   []int
		want []string
	}{
		{
			name: "empty",
		},
		{
			name: "non-empty",
			in:   []int{1, 2, 3},
			want: []string{"1", "2", "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			mapped := Map(func(i int) string {
				return fmt.Sprint(i)
			})(in)
			got := ToSlice(mapped)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestMapFilter(t *testing.T) {
	var tests = []struct {
		name string
		in   []Pair[int, bool]
		want []string
	}{
		{
			name: "empty",
		},
		{
			name: "non-empty",
			in:   []Pair[int, bool]{{1, true}, {2, false}, {3, true}},
			want: []string{"1", "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			mapped := MapFilter(func(i Pair[int, bool]) (string, bool) {
				return fmt.Sprint(i.A), i.B
			})(in)
			got := ToSlice(mapped)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestMapCancel(t *testing.T) {
	var tests = []struct {
		name       string
		in         []Pair[int, bool]
		wantCancel bool
		want       []string
	}{
		{
			name: "empty",
		},
		{
			name:       "cancelled",
			in:         []Pair[int, bool]{{1, true}, {2, false}, {3, true}},
			wantCancel: true,
			want:       []string{"1"},
		},
		{
			name:       "non-cancelled",
			in:         []Pair[int, bool]{{1, true}, {2, true}, {3, true}},
			wantCancel: false,
			want:       []string{"1", "2", "3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			var cancelled bool
			cncl := func() { cancelled = true }
			mapped := MapCancel(func(i Pair[int, bool]) (string, bool) {
				return fmt.Sprint(i.A), i.B
			}, cncl)(in)
			got := ToSlice(mapped)
			if cancelled != tt.wantCancel {
				t.Errorf("cancel: got: %v, want: %v", cancelled, tt.wantCancel)
			}
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestMapFilterCancel(t *testing.T) {
	var tests = []struct {
		name       string
		in         []Triplet[int, bool, bool]
		wantCancel bool
		want       []string
	}{
		{
			name: "empty",
		},
		{
			name:       "cancelled",
			in:         []Triplet[int, bool, bool]{{1, true, true}, {2, false, true}, {3, true, false}, {4, true, true}},
			wantCancel: true,
			want:       []string{"1", "3"},
		},
		{
			name:       "non-cancelled",
			in:         []Triplet[int, bool, bool]{{1, true, true}, {2, false, true}, {3, true, true}, {4, true, true}},
			wantCancel: false,
			want:       []string{"1", "3", "4"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			var cancelled bool
			cncl := func() { cancelled = true }
			mapped := MapFilterCancel(func(i Triplet[int, bool, bool]) (string, bool, bool) {
				return fmt.Sprint(i.A), i.B, i.C
			}, cncl)(in)
			got := ToSlice(mapped)
			if cancelled != tt.wantCancel {
				t.Errorf("cancel: got: %v, want: %v", cancelled, tt.wantCancel)
			}
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestMapFilterCancelTeardown(t *testing.T) {
	var tests = []struct {
		name       string
		in         []Triplet[int, bool, bool]
		td         Pair[string, bool]
		wantLast   int
		wantCancel bool
		want       []string
	}{
		{
			name: "empty",
		},
		{
			name:       "cancelled",
			in:         []Triplet[int, bool, bool]{{1, true, true}, {2, false, true}, {3, true, false}, {4, true, true}},
			td:         Pair[string, bool]{"5", true},
			wantLast:   3,
			wantCancel: true,
			want:       []string{"1", "3", "5"},
		},
		{
			name:       "non-cancelled",
			in:         []Triplet[int, bool, bool]{{1, true, true}, {2, false, true}, {3, true, true}, {4, true, true}},
			td:         Pair[string, bool]{"5", false},
			wantLast:   4,
			wantCancel: false,
			want:       []string{"1", "3", "4"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			var cancelled bool
			cncl := func() { cancelled = true }
			var tdE bool
			mapped := MapFilterCancelTeardown(func(i Triplet[int, bool, bool]) (string, bool, bool) {
				return fmt.Sprint(i.A), i.B, i.C
			},
				cncl,
				func(l Triplet[int, bool, bool], emitted bool) (string, bool) {
					tdE = true
					if got, want := emitted, len(tt.in) != 0; got != want {
						t.Errorf("teardown emitted: got %v want %v", got, want)
					}
					if emitted {
						if got, want := l.A, tt.wantLast; got != want {
							t.Errorf("teardown last: got %v want %v", got, want)
						}
					}
					return tt.td.A, tt.td.B
				},
			)(in)
			got := ToSlice(mapped)
			if cancelled != tt.wantCancel {
				t.Errorf("cancel: got: %v, want: %v", cancelled, tt.wantCancel)
			}
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if !tdE {
				t.Errorf("Teardown did not execute")
			}
		})
	}
}

func TestParallelMap(t *testing.T) {
	var tests = []struct {
		name       string
		start, end int
	}{
		{
			name: "empty",
		},
		{
			name:  "non-empty",
			start: 1,
			end:   100,
		},
	}
	project := func(i int) string {
		return fmt.Sprint(i)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromRange(tt.start, tt.end)
			mapped := ParallelMap(10, project)(in())
			got := ToSlice(mapped)
			slices.Sort(got)
			want := ToSlice(Map(project)(in()))
			slices.Sort(want)
			if diff := cmpDiff(want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

// TODO: test ParallelMapCancel

func TestParallelStable(t *testing.T) {
	var tests = []struct {
		name       string
		start, end int
	}{
		{
			name: "empty",
		},
		{
			name:  "non-empty",
			start: 1,
			end:   100,
		},
	}
	var struggle bool
	project := func(i int) string {
		var a int
		if struggle {
			for i := 0; i < 1000; i++ {
				a = i + a*i
			}
		}
		_ = a
		return fmt.Sprint(i)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromRange(tt.start, tt.end)
			runner := func(par, win int) {
				mapped := ParallelMapStable(par, win, project)(in())
				got := ToSlice(mapped)
				want := ToSlice(Map(project)(in()))
				if diff := cmpDiff(want, got); diff != "" {
					t.Errorf("-want +got:\n%s", diff)
				}
			}
			struggle = false
			runner(10, 20)
			runner(20, 10)
			runner(1, 1)
			runner(100, 1)
			struggle = true
			runner(10, 20)
			runner(20, 10)
			runner(1, 1)
			runner(100, 1)
		})
	}
}

func TestMergeMap(t *testing.T) {

	// TODO: Test that we actually emit them in a meaningful order

	var tests = []struct {
		name       string
		start, end int
		want       []string
	}{
		{
			name: "empty",
		},
		{
			name:  "four",
			start: 0,
			end:   4,
			want:  []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"},
		},
	}
	project := func(i int) <-chan string {
		inner := FromRange(i*3+1, i*3+4)()
		return Map(func(i int) string { return fmt.Sprint(i) })(inner)
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ch := MergeMap(3, project)(FromRange(tt.start, tt.end)())
			got := ToSlice(ch)
			slices.Sort(got)
			slices.Sort(tt.want)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}
