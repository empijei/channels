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

package channels_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"golang.org/x/exp/slices"

	. "github.com/empijei/channels"
)

////////////////
// Test Utils //
////////////////

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

type stubClock chan time.Time

func newStubClock() stubClock {
	return make(chan time.Time)
}
func (s stubClock) After(time.Duration) <-chan time.Time {
	return s
}
func (s stubClock) Emit() {
	s <- time.Time{}
}

// TODO: find a way to check for goroutine leaks.

const parall = true

func parallel(t *testing.T) {
	if parall {
		t.Parallel()
	}
}

// TODO(clap): use this instead of cmp.Diff
// TODO(clap): talk about this
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

///////////
// Tests //
///////////

func TestFromFunc(t *testing.T) {
	parallel(t)
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

func TestFromReaderLines(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []string
	}{
		{
			name: "empty",
		},
		{
			name: "one",
			in:   []string{"a"},
		},
		{
			name: "multi",
			in:   []string{"a", "b", "cd", "foobar"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			instr := strings.Join(tt.in, "\n")
			errs := make(chan error, 1)
			lines := FromReaderLines(strings.NewReader(instr), errs)()
			got := ToSliceParallel(lines)
			if diff := cmpDiff(tt.in, <-got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if err := <-errs; err != nil {
				t.Errorf("Unwanted err: %v", err)
			}
		})
	}
}

func TestFromFileLines(t *testing.T) {
	parallel(t)

	d := t.TempDir()
	f := filepath.Join(d, "TestFromFileLines")
	os.WriteFile(f, []byte("foo\nbar\ncat"), 0666)

	errp, errs := ErrorScope(nil)

	lines := FromFileLines(f, errp)()
	got := ToSliceParallel(lines)
	if diff := cmpDiff([]string{"foo", "bar", "cat"}, <-got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
	if err := EndScope(errp, errs); err != nil {
		t.Errorf("Unwanted err: %v", err)
	}
}

func TestToSlice(t *testing.T) {
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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

func TestCombineLatest(t *testing.T) {
	parallel(t)
	a := make(chan int)
	b := make(chan int)
	got := ToSliceParallel(CombineLatest[int, int]()(a, b))
	a <- 1 // Discarded
	a <- 2 // Buffered
	b <- 3 // [2,3]
	a <- 4 // [4, 3]
	a <- 5 //[5, 3]
	b <- 6 // [5, 6]
	close(a)
	flush()
	b <- 7 // [5, 7]
	close(b)
	want := []Pair[int, int]{{2, 3}, {4, 3}, {5, 3}, {5, 6}, {5, 7}}
	if diff := cmpDiff(want, <-got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestConcat(t *testing.T) {
	parallel(t)
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
	parallel(t)

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
	parallel(t)
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
	parallel(t)

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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	close(in) // [7] Leftorvers should be emitted

	got := <-gotch

	want := [][]int{{1, 2}, {3}, {4, 5, 6}, {7}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestBufferTimeOrCount(t *testing.T) {
	parallel(t)
	s := newStubTicker()
	f := newStubTickerFactory(t, 1*time.Second, s)
	now := time.Now()
	emit := func() {
		now = now.Add(1 * time.Second)
		s <- now
	}
	buffered := BufferTimeOrCount[int](f, 1*time.Second, 2)
	in := make(chan int)
	gotch := ToSliceParallel(buffered(in))

	emit() // Attempt empty emission, should be absorbed.
	in <- 1
	in <- 2 // [1, 2]
	in <- 3
	emit() // [3]
	in <- 4
	in <- 5 // [4, 5]
	in <- 6
	emit() // [6]
	in <- 7
	close(in) // [7] Leftorvers should be emitted

	got := <-gotch

	want := [][]int{{1, 2}, {3}, {4, 5}, {6}, {7}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestBufferToggle(t *testing.T) {
	parallel(t)
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

func TestBufferChan(t *testing.T) {
	parallel(t)

	in := make(chan int)
	out := BufferChan[int](5)(in)
	for i := 0; i < 5; i++ {
		in <- i
	}
	close(in)
	got := ToSlice(out)

	if diff := cmpDiff([]int{0, 1, 2, 3, 4}, got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

// TODO: TestConcatMap

func TestExhaustMap(t *testing.T) {
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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
				cncl,
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
	parallel(t)
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
	parallel(t)
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
	parallel(t)
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

func TestPairWise(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want [][2]int
	}{
		{
			name: "empty",
		},
		{
			name: "three",
			in:   []int{1, 2, 3, 4},
			want: [][2]int{{1, 2}, {2, 3}, {3, 4}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToSlice(PairWise[int]()(FromSlice(tt.in)()))
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestScan(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		seed int
		in   []int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "four",
			in:   []int{1, 2, 3, 4},
			want: []int{1, 3, 6, 10},
		},
		{
			name: "with seed",
			seed: 1,
			in:   []int{2, 3, 4},
			want: []int{3, 6, 10},
		},
	}

	project := func(acc, i int) int {
		return acc + i
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToSlice(Scan(project, tt.seed)(FromSlice(tt.in)()))
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestScanPreamble(t *testing.T) {
	parallel(t)
	project := func(acc, i int) (int, int) {
		return acc + i, acc + i
	}

	var tests = []struct {
		name   string
		seedFn func(int) (int, int)
		in     []int
		want   []int
	}{
		{
			name:   "four",
			seedFn: func(i int) (int, int) { return project(4, i) },
			in:     []int{1, 2, 3, 4},
			want:   []int{5, 7, 10, 14},
		},
		{
			name:   "first is seed",
			seedFn: func(i int) (int, int) { return project(i, i) },
			in:     []int{1, 2, 3},
			want:   []int{2, 4, 7},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ToSlice(ScanPreamble(project, tt.seedFn)(FromSlice(tt.in)()))
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestSwitchMap(t *testing.T) {
	parallel(t)
	inners := make(chan (chan string))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var mu sync.Mutex
	var calls []int
	var innerCtx context.Context

	cancelled := func() bool {
		select {
		case <-innerCtx.Done():
			return true
		default:
			return false
		}
	}

	project := func(ctx context.Context, i int) <-chan string {
		mu.Lock()
		calls = append(calls, i)
		innerCtx = ctx
		mu.Unlock()
		return <-inners
	}

	in := make(chan int)
	mapped := SwitchMap(ctx, project)
	gotch := ToSliceParallel(mapped(in))

	{
		prj := make(chan string)
		in <- 1       // project(1)
		inners <- prj // Unblock call to project
		prj <- "a"    // "a"
		prj <- "b"    // "b"
		close(prj)    // conclude
		if cancelled() {
			t.Errorf("After inner exhaustion: cancelled: got true, want false")
		}
	}
	{
		prj0 := make(chan string)
		in <- 2
		inners <- prj0
		prj0 <- "c"
		prj0 <- "d"
		ctx0 := innerCtx
		in <- 3 // This should cancel the context, discard prj0, create a new one and abort inner emission
		// Wait for cancellation
		<-ctx0.Done()
		prj0 <- "ignored"
		close(prj0) // conclude inner

		prj1 := make(chan string)
		inners <- prj1 // Unblock call to project.
		if cancelled() {
			t.Errorf("After new emission: cancelled: got true, want false")
		}
		ctx1 := innerCtx
		// No emissions
		in <- 4 // Discard prj1
		prj2 := make(chan string)
		inners <- prj2 // Unblock call to project.
		close(prj1)
		if cancelled() {
			t.Errorf("After new emission: cancelled: got true, want false")
		}
		// Wait for cancellation
		<-ctx1.Done()
		prj2 <- "e"
		close(prj2) // conclude inner
	}
	close(in)

	got := <-gotch
	want := []string{"a", "b", "c", "d", "e"}

	mu.Lock()
	defer mu.Unlock()
	if diff := cmpDiff([]int{1, 2, 3, 4}, calls); diff != "" {
		t.Errorf("project calls: -want +got:\n%s", diff)
	}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("values: -want +got:\n%s", diff)
	}
}

func TestWindow(t *testing.T) {
	parallel(t)
	in := make(chan int)
	emitCh := make(chan struct{})
	done := make(chan struct{})
	emit := func() { emitCh <- struct{}{} }

	ws := Window[int](emitCh)(in)

	var got [][]int
	go func() {
		defer close(done)
		for w := range ws {
			var inner []int
			for v := range w {
				inner = append(inner, v)
			}
			got = append(got, inner)
		}
	}()

	in <- 1
	in <- 2
	emit()
	in <- 3
	in <- 4
	emit()
	emit()
	in <- 5
	close(in)
	<-done
	want := [][]int{{1, 2}, {3, 4} /* discard empty*/, {5}}

	if diff := cmpDiff(want, got); diff != "" {
		t.Errorf("values: -want +got:\n%s", diff)
	}
}

func TestWindowCount(t *testing.T) {
	parallel(t)

	var tests = []struct {
		name string
		s, e int
		want [][]int
	}{
		{
			name: "empty",
		},
		{
			name: "exact dividend",
			s:    0,
			e:    9,
			want: [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}},
		},
		{
			name: "leftovers",
			s:    0,
			e:    11,
			want: [][]int{{0, 1, 2}, {3, 4, 5}, {6, 7, 8}, {9, 10}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromRange(tt.s, tt.e)
			ws := WindowCount[int](3)(in())
			var got [][]int
			for w := range ws {
				g := ToSlice(w)
				got = append(got, g)
			}
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestAudit(t *testing.T) {
	parallel(t)
	in := make(chan int)
	emitCh := make(chan struct{})
	au := Audit[int](emitCh)(in)

	c := 0
	obs := func(want int) {
		t.Helper()
		emitCh <- struct{}{}
		got := <-au
		c++
		if got != want {
			t.Errorf("audited[%v]: got %v want %v", c, got, want)
		}
	}

	emitCh <- struct{}{} // This should not emit because we don't have an initial value
	in <- 1
	obs(1)
	in <- 2
	obs(2)
	obs(2)
	in <- 3
	in <- 4
	obs(4)
	close(in)
	for range au {
	} // Make sure we close the output
}

func TestFilter(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "non-empty",
			in:   []int{0, 1, 2, 3, 4},
			want: []int{1, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := Filter(func(i int) bool {
				return i%2 != 0
			})(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestFilterCancel(t *testing.T) {
	parallel(t)
	t.Run("no cancel", func(t *testing.T) {
		in := FromRange(0, 6)()
		var gotCancel bool
		cncl := func() { gotCancel = true }
		flt := FilterCancel(func(i int) (e bool, l bool) {
			return i%2 != 0, false
		}, cncl)(in)
		got := ToSlice(flt)
		if diff := cmpDiff([]int{1, 3, 5}, got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if gotCancel {
			t.Errorf("got unwanted cancel")
		}
	})
	t.Run("cancel", func(t *testing.T) {
		var mu sync.Mutex
		var gotCancel bool
		var last bool
		in := make(chan int)
		done := make(chan struct{})
		step := make(chan struct{}, 1)
		cncl := func() {
			gotCancel = true
			close(done)
		}
		flt := FilterCancel(func(i int) (e bool, l bool) {
			mu.Lock()
			defer mu.Unlock()
			step <- struct{}{}
			return i%2 != 0, last
		}, cncl)(in)
		got := ToSliceParallel(flt)
		in <- 0
		<-step
		in <- 1
		<-step
		in <- 2
		<-step
		mu.Lock()
		last = true
		mu.Unlock()
		in <- 3
		<-done
		in <- 4 //ignored
		in <- 5 //ignored
		// Make sure we already closed the output
		<-flt
		in <- 6 //ignored
		in <- 7 //ignored
		close(in)

		if diff := cmpDiff([]int{1, 3}, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if !gotCancel {
			t.Errorf("didn't get wanted cancel")
		}
	})
}

func TestDistinct(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "non-empty",
			in:   []int{0, 1, 2, 2, 3, 1, 4},
			want: []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := Distinct[int]()(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestDistinctUntilChanged(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "non-empty",
			in:   []int{0, 1, 2, 2, 3, 1, 1, 4},
			want: []int{0, 1, 2, 3, 1, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := DistinctUntilChanged[int]()(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestAt(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name  string
		in    []int
		index int
		want  []int
	}{
		{
			name: "empty",
		},
		{
			name:  "empty, nonzero index",
			index: 10,
		},
		{
			name:  "last",
			index: 4,
			in:    []int{0, 1, 2, 3, 4},
			want:  []int{4},
		},
		{
			name:  "too short",
			index: 4,
			in:    []int{0, 1, 2, 3},
		},
		{
			name:  "in the middle",
			index: 4,
			in:    []int{0, 1, 2, 3, 4, 5, 67, 8, 9, 10},
			want:  []int{4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cancelled bool
			cncl := func() { cancelled = true }
			in := FromSlice(tt.in)()
			flt := At[int](tt.index, cncl)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if len(tt.want) != 0 != cancelled {
				t.Errorf("cancelled: got %v want %v", cancelled, len(tt.want) != 0)
			}
		})
	}
}

func TestAtWithDefault(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name       string
		in         []int
		index, def int
		wantCancel bool
		want       []int
	}{
		{
			name: "empty",
			def:  5,
			want: []int{5},
		},
		{
			name:  "empty, nonzero index",
			index: 10,
			def:   5,
			want:  []int{5},
		},
		{
			name:       "last",
			index:      4,
			def:        42,
			in:         []int{0, 1, 2, 3, 4},
			want:       []int{4},
			wantCancel: true,
		},
		{
			name:  "too short",
			index: 4,
			def:   42,
			in:    []int{0, 1, 2, 3},
			want:  []int{42},
		},
		{
			name:       "in the middle",
			index:      4,
			def:        42,
			in:         []int{0, 1, 2, 3, 4, 5, 67, 8, 9, 10},
			want:       []int{4},
			wantCancel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cancelled bool
			cncl := func() { cancelled = true }
			in := FromSlice(tt.in)()
			flt := AtWithDefault(tt.index, tt.def, cncl)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if tt.wantCancel != cancelled {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestTake(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name       string
		in         []int
		count      int
		want       []int
		wantCancel bool
	}{
		{
			name: "empty",
		},
		{
			name:  "empty, nonzero count",
			count: 4,
		},
		{
			name:       "part of it",
			count:      3,
			in:         []int{0, 1, 2, 3, 4},
			want:       []int{0, 1, 2},
			wantCancel: true,
		},
		{
			name:  "input too short",
			count: 5,
			in:    []int{0, 1, 2},
			want:  []int{0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cancelled bool
			cncl := func() { cancelled = true }
			in := FromSlice(tt.in)()
			flt := Take[int](tt.count, cncl)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if tt.wantCancel != cancelled {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestTakeUntil(t *testing.T) {
	parallel(t)
	t.Run("with partial emissions", func(t *testing.T) {
		var cancelled bool
		cncl := func() { cancelled = true }
		in := make(chan int)
		emit := make(chan struct{})
		tu := TakeUntil[int](emit, cncl)(in)
		got := ToSliceParallel(tu)
		in <- 1
		in <- 2
		emit <- struct{}{}
		in <- 3
		in <- 4
		close(in)
		want := []int{1, 2}
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if !cancelled {
			t.Errorf("cancelled: got false want true")
		}
	})
	t.Run("no emissions", func(t *testing.T) {
		var cancelled bool
		cncl := func() { cancelled = true }
		in := make(chan int)
		emit := make(chan struct{})
		tu := TakeUntil[int](emit, cncl)(in)
		got := ToSliceParallel(tu)
		emit <- struct{}{}
		close(in)
		var want []int
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if !cancelled {
			t.Errorf("cancelled: got false want true")
		}
	})
	t.Run("no cancel", func(t *testing.T) {
		var cancelled bool
		cncl := func() { cancelled = true }
		in := make(chan int)
		emit := make(chan struct{})
		tu := TakeUntil[int](emit, cncl)(in)
		got := ToSliceParallel(tu)
		in <- 1
		in <- 2
		in <- 3
		in <- 4
		close(in)
		want := []int{1, 2, 3, 4}
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if cancelled {
			t.Errorf("cancelled: got true want false")
		}
	})
	// TODO no interruption
}

func TestFirst(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name       string
		in         []int
		pred       func(int) bool
		want       []int
		wantCancel bool
	}{
		{
			name: "empty",
		},
		{
			name: "empty, non nil pred",
			pred: func(int) bool { return false },
		},
		{
			name:       "multiple matches",
			pred:       func(i int) bool { return i%2 != 0 },
			in:         []int{0, 1, 2, 3, 4},
			want:       []int{1},
			wantCancel: true,
		},
		{
			name:       "no pred",
			in:         []int{0, 1, 2, 3, 4},
			want:       []int{0},
			wantCancel: true,
		},
		{
			name: "no match",
			pred: func(i int) bool { return false },
			in:   []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cancelled bool
			cncl := func() { cancelled = true }
			in := FromSlice(tt.in)()
			flt := First(tt.pred, cncl)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if tt.wantCancel != cancelled {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestLast(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		pred func(int) bool
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "empty, non nil pred",
			pred: func(int) bool { return false },
		},
		{
			name: "multiple matches",
			pred: func(i int) bool { return i%2 != 0 },
			in:   []int{0, 1, 2, 3, 4},
			want: []int{3},
		},
		{
			name: "multiple matches, last element",
			pred: func(i int) bool { return i%2 != 0 },
			in:   []int{0, 1, 2, 3, 4, 5},
			want: []int{5},
		},
		{
			name: "no pred",
			in:   []int{0, 1, 2, 3, 4},
			want: []int{4},
		},
		{
			name: "no match",
			pred: func(i int) bool { return false },
			in:   []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := Last(tt.pred)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestSample(t *testing.T) {
	parallel(t)
	in := make(chan int)
	emit := make(chan struct{})
	e := func() { emit <- struct{}{} }
	sampled := Sample[int](emit)(in)
	got := ToSliceParallel(sampled)

	e() // Ignored
	in <- 1
	e() // 1
	in <- 2
	in <- 3
	e() // 3
	e() // Ignored
	in <- 4
	close(in)
	want := []int{1, 3}

	if diff := cmpDiff(want, <-got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
}

func TestSkip(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name  string
		in    []int
		count int
		want  []int
	}{
		{
			name: "empty",
		},
		{
			name:  "empty, nonzero count",
			count: 4,
		},
		{
			name:  "part of it",
			count: 3,
			in:    []int{0, 1, 2, 3, 4},
			want:  []int{3, 4},
		},
		{
			name:  "input too short",
			count: 5,
			in:    []int{0, 1, 2},
		},
		{
			name:  "no skip",
			count: 0,
			in:    []int{0, 1, 2, 3, 4},
			want:  []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := Skip[int](tt.count)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestSkipLast(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name  string
		in    []int
		count int
		want  []int
	}{
		{
			name: "empty",
		},
		{
			name:  "empty, nonzero count",
			count: 4,
		},
		{
			name:  "part of it",
			count: 3,
			in:    []int{0, 1, 2, 3, 4},
			want:  []int{0, 1},
		},
		{
			name:  "input too short",
			count: 5,
			in:    []int{0, 1, 2},
		},
		{
			name:  "no skip",
			count: 0,
			in:    []int{0, 1, 2, 3, 4},
			want:  []int{0, 1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := SkipLast[int](tt.count)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestStartWith(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name  string
		in    []int
		start int
		want  []int
	}{
		{
			name:  "empty",
			start: 1,
			want:  []int{1},
		},
		{
			name:  "non empty",
			start: 1,
			in:    []int{2, 3, 4},
			want:  []int{1, 2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			flt := StartWith(tt.start)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

// TODO: WithLatestFrom

func TestWithLatestFrom(t *testing.T) {
	parallel(t)
	in := make(chan int)
	other := make(chan string)
	cancelled := false
	cncl := func() { cancelled = true }
	wlf := WithLatestFrom[int](other, cncl)(in)
	got := ToSliceParallel(wlf)
	in <- 0      // Discarded
	in <- 1      // Discarded
	other <- "2" // Discarded
	in <- 3      // [3, "2"]
	in <- 4      // [4, "2"]
	other <- "5" // Buffered
	in <- 6      // [6, "5"]
	close(in)
	want := []Pair[int, string]{{3, "2"}, {4, "2"}, {6, "5"}}
	if diff := cmpDiff(want, <-got); diff != "" {
		t.Errorf("-want +got:\n%s", diff)
	}
	if !cancelled {
		t.Error("other didn't get cancelled")
	}
}

func TestTap(t *testing.T) {
	parallel(t)

	var tests = []struct {
		name string
		in   []int
	}{
		{
			name: "empty",
		},
		{
			name: "non empty",
			in:   []int{2, 3, 4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			var acc []int
			accum := func(i int) { acc = append(acc, i) }
			flt := Tap(accum)(in)
			got := ToSlice(flt)
			if diff := cmpDiff(tt.in, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if diff := cmpDiff(tt.in, acc); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestTimeout(t *testing.T) {
	parallel(t)
	t.Run("expired", func(t *testing.T) {
		c := newStubClock()
		cancelled := false
		cncl := func() { cancelled = true }
		in := make(chan int)
		to := Timeout[int](c, 1*time.Minute, cncl)(in)
		got := ToSliceParallel(to)
		c.Emit() // Timed out
		var want []int
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if !cancelled {
			t.Error("didn't cancel expired emitter")
		}
	})
	t.Run("not expired", func(t *testing.T) {
		c := newStubClock()
		cancelled := false
		cncl := func() { cancelled = true }
		in := make(chan int)
		to := Timeout[int](c, 1*time.Minute, cncl)(in)
		got := ToSliceParallel(to)
		in <- 1
		select {
		case c <- time.Time{}:
			// Ignored
		default:
		}
		in <- 2
		in <- 3
		close(in)
		want := []int{1, 2, 3}
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if cancelled {
			t.Error("got unwanted cancel")
		}
	})
}

func TestTeardown(t *testing.T) {
	parallel(t)
	t.Run("no emission", func(t *testing.T) {
		in := make(chan int)
		close(in)
		var last int
		var emitted bool
		out := Teardown(func(l int, e bool) (int, bool) {
			last = l
			emitted = e
			return 2, true
		})(in)

		got := ToSliceParallel(out)
		want := []int{2}
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if emitted {
			t.Error("emitted: got true want false")
		}
		if last != 0 {
			t.Errorf("last: got %v want 0", last)
		}
	})

	t.Run("emissions", func(t *testing.T) {
		in := make(chan int)
		var last int
		var emitted bool
		out := Teardown(func(l int, e bool) (int, bool) {
			last = l
			emitted = e
			return 4, false
		})(in)
		got := ToSliceParallel(out)
		in <- 1
		in <- 2
		in <- 3
		close(in)
		want := []int{1, 2, 3}
		if diff := cmpDiff(want, <-got); diff != "" {
			t.Errorf("-want +got:\n%s", diff)
		}
		if !emitted {
			t.Error("emitted: got false want true")
		}
		if last != 3 {
			t.Errorf("last: got %v want 0", last)
		}
	})
}

func TestEvery(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name             string
		in               []int
		want, wantCancel bool
	}{
		{
			name: "empty",
			want: true,
		},
		{
			name:       "non empty, not match",
			in:         []int{2, 3, 4},
			wantCancel: true,
		},
		{
			name: "non empty, match",
			in:   []int{2, 4, 6},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			cancelled := false
			res := Every(
				func(i int) bool { return i%2 == 0 },
				func() { cancelled = true },
			)(in)
			got := ToSlice(res)[0]
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if cancelled != tt.wantCancel {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestFindIndex(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name       string
		in         []int
		want       []int
		wantCancel bool
	}{
		{
			name: "empty",
		},
		{
			name: "non empty, not match",
			in:   []int{1, 3, 5},
		},
		{
			name:       "non empty, match",
			in:         []int{11, 12, 13},
			want:       []int{1},
			wantCancel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			cancelled := false
			res := FindIndex(
				func(i int) bool { return i%2 == 0 },
				func() { cancelled = true },
			)(in)
			got := ToSlice(res)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if cancelled != tt.wantCancel {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestIsEmpty(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name       string
		in         []int
		want       bool
		wantCancel bool
	}{
		{
			name: "empty",
			want: true,
		},
		{
			name:       "non empty, not match",
			in:         []int{1, 3, 5},
			want:       false,
			wantCancel: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			cancelled := false
			res := IsEmpty[int](func() { cancelled = true })(in)
			got := ToSlice(res)[0]
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
			if cancelled != tt.wantCancel {
				t.Errorf("cancelled: got %v want %v", cancelled, tt.wantCancel)
			}
		})
	}
}

func TestReduce(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want []int
	}{
		{
			name: "empty",
		},
		{
			name: "one",
			in:   []int{1},
			want: []int{1},
		},
		{
			name: "non empty",
			in:   []int{1, 2, 3},
			want: []int{6},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			res := Reduce(func(sum, cur int) int {
				return sum + cur
			}, 0)(in)
			got := ToSlice(res)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestCount(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name string
		in   []int
		want []int
	}{
		{
			name: "empty",
			want: []int{0},
		},
		{
			name: "one",
			in:   []int{1},
			want: []int{1},
		},
		{
			name: "non empty",
			in:   []int{1, 2, 3},
			want: []int{3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			res := Count[int]()(in)
			got := ToSlice(res)
			if diff := cmpDiff(tt.want, got); diff != "" {
				t.Errorf("-want +got:\n%s", diff)
			}
		})
	}
}

func TestTeeMinMax(t *testing.T) {
	parallel(t)
	var tests = []struct {
		name             string
		in               []int
		wantMin, wantMax []int
	}{
		{
			name: "empty",
		},
		{
			name:    "one",
			in:      []int{1},
			wantMin: []int{1},
			wantMax: []int{1},
		},
		{
			name:    "non empty",
			in:      []int{2, 1, 4, 3},
			wantMin: []int{1},
			wantMax: []int{4},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := FromSlice(tt.in)()
			a, b := Tee[int]()(in)
			min := Min[int]()(a)
			max := Max[int]()(b)
			gotMin := ToSliceParallel(min)
			gotMax := ToSliceParallel(max)
			if diff := cmpDiff(tt.wantMin, <-gotMin); diff != "" {
				t.Errorf("Min(%v): -want +got:\n%s", tt.in, diff)
			}
			if diff := cmpDiff(tt.wantMax, <-gotMax); diff != "" {
				t.Errorf("Max(%v): -want +got:\n%s", tt.in, diff)
			}
		})
	}
}
