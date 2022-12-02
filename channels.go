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

package channels

import (
	"context"
	"errors"
	"sync"
	"time"

	"golang.org/x/exp/constraints"
	"golang.org/x/sync/errgroup"
)

/*
TODO: point this out
type declarations inside generic functions are not currently supported

TODO: make ops that take an emitter also take a cancelParent that gets called when
emit is closed, and drain input.

TODO: make sure that wording like "forwards", "becomes a copy" etc. are consistently
used and well defined in the doc.

TODO: create a styleguide on when and where operators should be invoked.

type outT = struct {
a A
b B
}

TODO: potentially create or require contexts for inner stuff that might be discarded
and cancel them once they get discarded (e.g. for switchmap)

TODO: check for if statements for first or similar conditions: can we set the condition inside the if?

TODO: check that all tests run in parallel.

TODO: find the talk by Samir and see if we can solve the problem presented there with the
operators in this package.

TODO: check all outs are closed.

TODO: consistency check on wording for docs.

TODO: document that last values are kept and emitted whenever possible, or change code to not do it.

TODO: check if I forgot some discards

TODO: check that variables we close over are necessary

TODO: find a way to return contextx when spawning subroutines the caller does not control

TODO: inline composed operators

TODO: chech that types for emit are D instead of other letters

TODO: point out
  // error: generic type cannot be alias
	// MonoTypeOperator[T any] = Operator[T, T]

TODO: express operators in terms of other operators.

TODO: make time related operators get a clock in input

TODO: say that there is no reduce because we have closures

TODO: check that cancelParent is propagated

TODO: document that reusing a constructor might have unexpected results (e.g. distinct filters also stuff from previous runs).

TODO: figure out a consistent way for parameters passing (e.g. cancelParent, parallelism and stuff should all be in the same order,
	and cancelParent should probably be last).
*/

///////////
// Types //
///////////

// Type parameters naming convention:
// Input: I L M
// Same type for in and out: T U V
// Output: O P Q
// Ignored values: D E F
// If there are channels used as a couple (e.g. for zip operators)
// A and B might be used to denote the first and the second type for the tuple.

type (
	SourceOperator[T any] func() <-chan T
	SinkOperator[T any]   func(<-chan T)

	Operator[I, O any] func(<-chan I) <-chan O

	FanOutOperator[I, O any]       func(<-chan I) []<-chan O
	ZipOperator[A, B, O any]       func(<-chan A, <-chan B) <-chan O
	PartitionOperator[I, O, P any] func(<-chan I) (<-chan O, <-chan P)

	FanInOperator[I, O any] func(...<-chan I) <-chan O

	ParallelOperator[I, O any] func([]<-chan I) []<-chan O

	Pair[A, B any] struct {
		A A
		B B
	}

	Triplet[A, B, C any] struct {
		A A
		B B
		C C
	}
)

////////////////////////
// Creation Operators //
////////////////////////

// FromFunc uses the provided generator as a source for data, and emits all values
// until ok becomes false (the element returned when ok is false is not emitted).
func FromFunc[T any](generator func(index int) (t T, ok bool)) SourceOperator[T] {
	return func() <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for i := 0; ; i++ {
				t, ok := generator(i)
				if !ok {
					return
				}
				out <- t
			}
		}()
		return out
	}
}

// FromSlice emits all values in s and ends.
func FromSlice[T any](s []T) SourceOperator[T] {
	return func() <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for _, v := range s {
				out <- v
			}
		}()
		return out
	}
}

// FromTicker emits the current time at most max times, separated by a wait of duration.
// If the context is cancelled it stops.
// If tickerFactory is nil, the real time is used.
func FromTicker(ctx context.Context, tickerFactory func(time.Duration) Ticker, duration time.Duration, max int) SourceOperator[time.Time] {
	if tickerFactory == nil {
		tickerFactory = NewRealTicker
	}
	return func() <-chan time.Time {
		out := make(chan time.Time)
		go func() {
			t := tickerFactory(duration)

			defer t.Stop()
			defer close(out)

			for i := 0; i < max; i++ {
				var now time.Time

				select {
				case now = <-t.Chan():
				case <-ctx.Done():
					return
				}

				select {
				case out <- now:
				case <-ctx.Done():
					return
				}
			}
		}()
		return out
	}
}

// FromRange emits all values between start (included) and end (excluded).
func FromRange(start, end int) SourceOperator[int] {
	if start > end {
		panic("FromRange: start can't be after end")
	}
	return func() <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			for i := start; i < end; i++ {
				out <- i
			}
		}()
		return out
	}
}

/////////////////////////////
// Join Creation Operators //
/////////////////////////////

// CombineLatest emits pairs of the last values emitted by the given inputs.
//
// That means that if input a emits twice and input b emits once, the two subsequent
// emitted pairs will have the same value for B, but different ones for A.
//
// Emissions only start after both inputs have emitted at least once, all emissions
// before then for the input that emitted first are discarded except for the last value.
//
// CombineLatest ends when both inputs end.
func CombineLatest[A, B any]() ZipOperator[A, B, Pair[A, B]] {
	return func(a <-chan A, b <-chan B) <-chan Pair[A, B] {

		out := make(chan Pair[A, B])

		go func() {
			defer close(out)

			var (
				outS               Pair[A, B]
				aEmitted, bEmitted bool
			)
			for {
				// The two cases for this select are identical and symmetric.
				select {
				case gotA, ok := <-a:
					if !ok {
						// chan A has been closed.
						if b == nil {
							// chan B has also been closed, let's end.
							return
						}
						// chan B is still going, let's disable this case.
						a = nil
						continue
					}
					// Store the received value.
					outS.A = gotA
					aEmitted = true
					if bEmitted {
						// Both emitted at least once, let's send the current value.
						out <- outS
					}
				case gotB, ok := <-b:
					if !ok {
						// chan B has been closed.
						if a == nil {
							// chan A has also been closed, let's end.
							return
						}
						// chan A is still going, let's disable this case.
						b = nil
						continue
					}
					// Store the received value.
					outS.B = gotB
					bEmitted = true
					if aEmitted {
						// Both emitted at least once, let's send the current value.
						out <- outS
					}
				}
			}
		}()

		return out
	}
}

// Concat emits all values from all inner inputs, one after the other, exhausting
// the previous ones before consuming the next.
func Concat[T any]() Operator[<-chan T, T] {
	return func(chans <-chan (<-chan T)) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for c := range chans {
				for v := range c {
					out <- v
				}
			}
		}()
		return out
	}
}

func ForkJoin[A, B any]() ZipOperator[A, B, Pair[A, B]] {
	return func(a <-chan A, b <-chan B) <-chan Pair[A, B] {
		o := CombineLatest[A, B]()(a, b)
		return Last[Pair[A, B]](nil)(o)
	}
}

// Merge concurrently reads all inner inputs and emits them.
// Oder is not preserved.
func Merge[T any]() Operator[<-chan T, T] {
	return func(chans <-chan (<-chan T)) <-chan T {
		out := make(chan T)

		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chans {
				c := c
				wg.Add(1)
				go func() {
					defer wg.Done()
					for v := range c {
						out <- v
					}
				}()
			}
		}()

		go func() {
			wg.Wait()
			close(out)
		}()
		return out
	}
}

// Partition emits values that match the condition on the first output (then)
// and the values that don't on the second output (elze).
func Partition[T any](condition func(t T) bool) PartitionOperator[T, T, T] {
	if condition == nil {
		panic("Partition: condition cannot be nil.")
	}
	return func(in <-chan T) (then, elze <-chan T) {
		th := make(chan T)
		el := make(chan T)

		go func() {
			defer close(th)
			defer close(el)
			for v := range in {
				if condition(v) {
					th <- v
				} else {
					el <- v
				}
			}
		}()
		return th, el
	}
}

// Race races the inputs and becomes a clone of the first one that emits,
// cancelling all the others.
func Race[T any](cancels ...func()) FanInOperator[T, T] {
	return func(chans ...<-chan T) <-chan T {
		out := make(chan T)

		var firstOnce sync.Once
		arrivedFirst := func() bool {
			isFirst := false
			firstOnce.Do(func() { isFirst = true })
			return isFirst
		}

		cancelOthers := func(cur int) {
			// Cancel all other competitors
			for ci, cancel := range cancels {
				if ci == cur {
					// This is the winner, do not cancel
					continue
				}
				cancel()
			}
		}

		for i, c := range chans {
			i := i
			c := c
			go func() {
				firstIteration := true
				for v := range c {
					if !firstIteration {
						// We won the race in a previous iteration,
						// we are responsible to write to the out chan.
						out <- v
						continue
					}

					firstIteration = false
					if arrivedFirst() {
						// We won the race, we are responsible to close the out chan once we are done.
						defer close(out) //nolint:staticcheck // this is intended to run at the end of the function.
						cancelOthers(i)
						out <- v
						continue
					}

					// We lost the race, discard input and return.
					drain(c, nil) // The winner should have already cancelled us
					return
				}
			}()
		}
		return out
	}
}

// Zip emits pairs of values from the inputs.
// Once a value is received, it waits for the other input to emit one before
// reading more from the same.
//
// In other words: every value from both inputs is guaranteed to be emitted
// exactly once in exactly one pair until one input is closed.
//
// If one input is closed, the other is cancelled and discarded.
//
// If both inputs have the same length, the second one to be closed might get
// cancelled right after its last emission, which should be a no-op.
func Zip[A, B any](cancelA, cancelB func()) ZipOperator[A, B, Pair[A, B]] {
	return func(a <-chan A, b <-chan B) <-chan Pair[A, B] {

		out := make(chan Pair[A, B])

		go func() {
			defer close(out)

			var outS Pair[A, B]
			inA := a
			inB := b
			for {
				// The two cases for this select are identical and symmetric.
				select {
				case gotA, ok := <-inA:
					if !ok {
						// TODO: only do this if needed
						// chan A was closed, let's just consume B and end.
						drain(b, cancelB)
						return
					}
					// Store the received value
					outS.A = gotA
					if inB == nil {
						// chan A emitted second for this pair, let's emit and re-enable both cases.
						out <- outS // This send copies the current value for outS.
						outS = Pair[A, B]{}
						inB = b
						continue
					}
					// chan A emitted first for this pair. Let's disable this case to wait
					// for B to emit.
					inA = nil
				case gotB, ok := <-inB:
					if !ok {
						// chan B was closed, let's just consume A and end.
						drain(a, cancelA)
						return
					}
					// Store the received value
					outS.B = gotB
					if inA == nil {
						// chan B emitted second for this pair, let's emit and re-enable both cases.
						out <- outS // This send copies the current value for outS.
						outS = Pair[A, B]{}
						inA = a
						continue
					}
					// chan B emitted first for this pair. Let's disable this case to wait
					// for A to emit.
					inB = nil
				}
			}
		}()

		return out
	}
}

//////////////////////////////
// Transformation Operators //
//////////////////////////////

// Buffer buffers elements from the input until the emitter emits, emitting a
// slice of the values seen between two emissions (or the start and the first emission).
//
// Emitted slices preserve the order of receiving.
//
// If the emitter emits when the buffer is empty, nothing is emitted.
//
// If the emitter or the input are closed when there are still elements in the buffer,
// those elements are emitted immediately and the output is closed.
func Buffer[T, D any](emit <-chan D) Operator[T, []T] {
	return func(in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)

			var buf []T
			emitBuf := func() {
				if len(buf) == 0 {
					return
				}
				out <- buf
				buf = nil
			}

			for {
				select {
				case v, ok := <-in:
					if !ok {
						// Input is closed, emit the last values (if any) and exit.
						emitBuf()
						return
					}
					buf = append(buf, v)
				case _, ok := <-emit:
					emitBuf()
					if !ok {
						// TODO: drain and cancel input?
						// Emitter is closed, exit.
						return
					}
				}
			}
		}()

		return out
	}
}

// BufferCount collects elements from the input and emits them in batches of length
// equal to count.
// If there are still elements in the buffer when the input is closed, one last,
// shorter, batch is emitted.
// Any count value smaller than 1 is treated as 1.
func BufferCount[T any](count int) Operator[T, []T] {
	if count <= 0 {
		count = 1
	}
	return func(in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)

			buf := make([]T, 0, count)
			for v := range in {
				buf = append(buf, v)
				if len(buf) >= count {
					out <- buf
					buf = make([]T, 0, count)
				}
			}
			if len(buf) != 0 {
				out <- buf
			}
		}()
		return out
	}
}

// BufferTime is like Buffer, but it uses a ticker to decide when to emit.
// If tickerFactory is nil, the real time is used.
func BufferTime[T any](tickerFactory func(time.Duration) Ticker, duration time.Duration) Operator[T, []T] {
	if tickerFactory == nil {
		tickerFactory = NewRealTicker
	}
	return func(in <-chan T) <-chan []T {
		t := tickerFactory(duration)
		emit := t.Chan()
		out := make(chan []T)
		go func() {
			defer close(out)
			defer t.Stop()

			var buf []T
			emitBuf := func() {
				if len(buf) == 0 {
					return
				}
				out <- buf
				buf = nil
			}

			for {
				select {
				case v, ok := <-in:
					if !ok {
						// Input is closed, emit the last values (if any) and exit.
						emitBuf()
						return
					}
					buf = append(buf, v)
				case _, ok := <-emit:
					emitBuf()
					if !ok {
						// Emitter is closed, exit.
						return
					}
				}
			}
		}()

		return out
	}
}

// BufferToggle buffers elements between an emission of openings and an emission of closings.
// Two consecutive emissions of openings or closings are ignored.
// If the buffer is open when any of the inputs ends the leftovers are emitted without
// an emission of closings.
// Elements between an emission of closings and an emission of openings are ignored.
func BufferToggle[T, I1, I2 any](openings <-chan I1, closings <-chan I2) Operator[T, []T] {
	return func(in <-chan T) <-chan []T {
		out := make(chan []T)
		go func() {
			defer close(out)

			var buf []T
			emitBuf := func() {
				if len(buf) == 0 {
					return
				}
				out <- buf
				buf = nil
			}
			defer emitBuf()

			open := false
			for {
				select {
				case _, ok := <-openings:
					if !ok {
						return
					}
					open = true
				case _, ok := <-closings:
					if !ok {
						return
					}
					if !open {
						continue
					}
					emitBuf()
					open = false
				case v, ok := <-in:
					if !ok {
						return
					}
					if !open {
						continue
					}
					buf = append(buf, v)
				}
			}

		}()

		return out
	}
}

func ConcatMap[I, O any](project func(in I) <-chan O) Operator[I, O] {
	return Combine(
		Map(project),
		Concat[O](),
	)
}

// ExhaustMap project inputs to inner emitters and forwards all inner emissions to
// the output.
//
// If the input emits while an inner emitter is being consumed, the value is discarded.
// In summary: this operator prioritizes inputs from the projected emitters over the
// input, making the input lossy.
//
// This is like SwitchMap, but the inner emitters are prioritized.
func ExhaustMap[I, O any](project func(in I) <-chan O) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		go func() {
			defer close(out)
			var inner <-chan O
			for {
				select {
				case v, ok := <-in:
					if !ok {
						if inner == nil {
							// We are done.
							return
						}
						in = nil
						continue
					}
					if inner != nil {
						// We are still consuming inner, discard update.
						continue
					}
					inner = project(v)
				case v, ok := <-inner:
					if !ok {
						// TODO add a spy here to deflake test?
						if in == nil {
							// We are done.
							return
						}
						// Exhausted inner, disable this case.
						inner = nil
						continue
					}
					out <- v
				}
			}
		}()

		return out
	}
}

// Map projects inputs to outputs.
func Map[I, O any](project func(in I) O) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			for v := range in {
				out <- project(v)
			}
		}()
		return out
	}
}

// MapFilter conditionally projects inputs to outputs.
// If the project func returns false as a second return value, that emission is skipped.
func MapFilter[I, O any](project func(in I) (projected O, emit bool)) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			for v := range in {
				prj, ok := project(v)
				if !ok {
					continue
				}
				out <- prj
			}
		}()
		return out
	}
}

// MapCancel projects inputs to outputs until the first not-ok value is reached,
// it then cancels the parent and drains the input.
// The not-ok value returned is discarded.
func MapCancel[I, O any](project func(in I) (projected O, ok bool), cancelParent func()) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			for v := range in {
				prj, ok := project(v)
				if !ok {
					drain(in, cancelParent)
					return
				}
				out <- prj
			}
		}()
		return out
	}
}

// MapFilterCancel conditionally projects inputs to outputs until the first non-ok
// value is reached, it then cancels the parent and drains the input.
// It is like a combination of MapFilter and MapCancel.
func MapFilterCancel[I, O any](project func(in I) (projected O, emit, ok bool), cancelParent func()) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			for v := range in {
				prj, emit, ok := project(v)
				if emit {
					out <- prj
				}
				if !ok {
					drain(in, cancelParent)
					return
				}
			}
		}()
		return out
	}
}

// MapFilterCancelTeardown conditionally projects inputs to outputs until the last
// value is reached, it then cancels the parent, drains the input and runs the
// teardown routine.
// It is like a combination of MapFilter, MapCancel and Teardown.
func MapFilterCancelTeardown[I, O any](
	project func(in I) (projected O, emit, ok bool),
	cancelParent func(),
	teardown func(last I, emitted bool) (lastItem O, shouldEmitLast bool),
) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			var emitted bool
			var last I
			for v := range in {
				emitted = true
				last = v
				t, emit, ok := project(v)
				if emit {
					out <- t
				}
				if !ok {
					drain(in, cancelParent)
					break
				}
			}
			if teardown == nil {
				return
			}
			l, e := teardown(last, emitted)
			if !e {
				return
			}
			out <- l
		}()
		return out
	}
}

// ParallelMap is like Map but runs the project function in parallel.
// Output order is not guaranteed to be related to input order.
func ParallelMap[I, O any](maxParallelism int, project func(in I) O) Operator[I, O] {
	if maxParallelism == 0 {
		panic("ParallelMap: maxParallelism must be positive")
	}
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			var wg sync.WaitGroup
			wg.Add(maxParallelism)
			for i := 0; i < maxParallelism; i++ {
				go func() {
					defer wg.Done()
					for v := range in {
						o := project(v)
						out <- o
					}
				}()
			}
			wg.Wait()
		}()

		return out
	}
}

// ParallelMapCancel is like ParallelMap, but allows the project function to abort operations.
func ParallelMapCancel[I, O any](maxParallelism int, project func(context.Context, I) (o O, ok bool), cancelParent func()) Operator[I, O] {
	if maxParallelism == 0 {
		panic("ParallelMapCancel: maxParallelism must be positive")
	}
	return func(in <-chan I) <-chan O {
		wg, ctx := errgroup.WithContext(context.Background())
		out := make(chan O)

		go func() {
			defer close(out)

			for i := 0; i < maxParallelism; i++ {
				for v := range in {
					v := v
					wg.Go(func() error {
						o, ok := project(ctx, v)
						if !ok {
							return errors.New("")
						}
						select {
						case out <- o:
						case <-ctx.Done():
							return ctx.Err()
						}
						return nil
					})
				}
			}

			if err := wg.Wait(); err != nil {
				drain(in, cancelParent)
			}
		}()

		return out
	}
}

// ParallelMapStable is like ParallelMap, but it guarantees that the output is
// in the same order of the input.
// The maxWindow parameter is used to determine how large the sorting window should be.
// In other words: if the input at index N is still processing when the item at index
// N+maxWindow is read, ParallelMapStable will wait for item N to be done processing.
// Using a maxWindow smaller than maxParallelism is not useful as the maxParallelism will
// never be achieved.
func ParallelMapStable[I, O any](maxParallelism, maxWindow int, project func(in I) O) Operator[I, O] {
	if maxParallelism <= 0 {
		panic("ParallelMapStable: maxParallelism must be positive")
	}
	if maxWindow <= 0 {
		panic("ParallelMapStable: maxWindow must be positive")
	}
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		go func() {
			iIn := make(chan Pair[int, I])
			iOut := make(chan Pair[int, O])

			var tmu sync.RWMutex
			tch := make(chan struct{})
			close(tch)

			throttle := func() {
				tmu.RLock()
				tch := tch
				tmu.RUnlock()
				<-tch
			}

			checkThrottle := func(wasThrottling bool, windowSize int) (throttling bool) {
				if !wasThrottling {
					if windowSize < maxWindow {
						// No throttling.
						return false
					}
					// Start throttling.
					tmu.Lock()
					tch = make(chan struct{})
					tmu.Unlock()
					return true
				}
				if windowSize < maxWindow {
					// Stop throttling.
					tmu.Lock()
					close(tch)
					tmu.Unlock()
					return false
				}
				// Keep throttling.
				return true
			}

			// Producer/Throttler
			go func() {
				defer close(iIn)
				c := 0
				for v := range in {
					throttle()
					iIn <- Pair[int, I]{c, v}
					c++
				}
			}()

			// Consumer
			go func() {
				defer close(out)
				window := map[int]O{}
				cur := 0
				throttle := false
				for v := range iOut {
					if v.A != cur {
						window[v.A] = v.B
						throttle = checkThrottle(throttle, len(window))
						continue
					}
					out <- v.B
					for {
						cur++
						o, ok := window[cur]
						if !ok {
							break
						}
						out <- o
						delete(window, cur)
						throttle = checkThrottle(throttle, len(window))
					}
				}
				for ; len(window) > 0; cur++ {
					out <- window[cur]
					delete(window, cur)
				}
			}()

			// Mappers
			var wg sync.WaitGroup
			wg.Add(maxParallelism)
			for i := 0; i < maxParallelism; i++ {
				go func() {
					defer wg.Done()
					for p := range iIn {
						i, v := p.A, p.B
						o := project(v)
						iOut <- Pair[int, O]{i, o}
					}
				}()
			}
			wg.Wait()
			close(iOut)
		}()

		return out
	}
}

// MergeMap projects the input emissions to inner emitters that are then merged
// in the output.
// Order of the output is not guaranteed to be related to the order of the input.
func MergeMap[I, O any](maxParallelism int, project func(in I) <-chan O) Operator[I, O] {
	if maxParallelism <= 0 {
		panic("MergeMap: maxParallelism must be positive")
	}
	semaphore := make(chan struct{}, maxParallelism)
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			defer close(out)
			wg.Wait()
		}()

		go func() {
			defer wg.Done()
			for v := range in {
				semaphore <- struct{}{}
				inner := project(v)
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						<-semaphore
					}()
					for v := range inner {
						out <- v
					}
				}()
			}
		}()

		return out
	}
}

// PairWise emits couples of subcessive emissions.
// This means all values are emitted twice, once as the second item of the pair,
// and then as the first item of the pair (the oldest is the one at index 0, as
// if it was a sliding window of length 2.
// The only exceptions are the first and last items, which will only be emitted once.
func PairWise[T any]() Operator[T, [2]T] {
	var (
		buf         [2]T
		emittedOnce bool
	)
	return MapFilter(func(in T) (o [2]T, e bool) {
		if !emittedOnce {
			buf[1] = in
			emittedOnce = true
			return o, false
		}
		buf[0] = buf[1]
		buf[1] = in
		return buf, true
	})
}

// Scan is like Map, but the project function is called with the the last
// value emitted and the current value from the input.
// The first iteraion will be called with seed.
func Scan[I, O any](project func(accum O, cur I) O, seed O) Operator[I, O] {
	return ScanAccum(func(prevAcc O, i I) (acc O, v O) {
		acc = project(prevAcc, i)
		return acc, acc
	}, seed)
}

// ScanAccum is like Scan, but the accumulator is kept separate from the return
// value, and they can be of different types.
func ScanAccum[I, O, A any](project func(accum A, cur I) (nextAccum A, o O), seed A) Operator[I, O] {
	accum := seed
	return Map(func(i I) O {
		nextAccum, out := project(accum, i)
		accum = nextAccum
		return out
	})
}

// ScanPreamble is like ScanAccum, but instead of taking a seed value it computes
// the seed and the first emission by calling seedFn func on the first input value.
func ScanPreamble[I, O, A any](project func(accum A, in I) (A, O), seedFn func(in I) (A, O)) Operator[I, O] {
	var accum A
	emitted := false
	return ScanAccum(func(accum A, in I) (A, O) {
		if !emitted {
			emitted = true
			return seedFn(in)
		}
		return project(accum, in)
	}, accum /*ignored*/)
}

// SwitchMap projects the inputs to inner emitters and copies them to the output
// until the input emits a new value.
// When a new input value comes in, the last inner emitter's context is cancelled
// and its values are discarded in favor of the ones emitted by the most recent
// inner emitter.
// This is like ExhaustMap but the input is prioritized.
func SwitchMap[I, O any](ctx context.Context, project func(ctx context.Context, in I) <-chan O) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		go func() {
			defer close(out)
			var inner <-chan O
			var innerCtx context.Context
			var innerCancel func()
			for {
				select {
				case v, ok := <-in:
					// Ignore inner values in favor of fresher ones.
					drain(inner, innerCancel)
					if !ok {
						return
					}
					innerCtx, innerCancel = context.WithCancel(ctx)
					inner = project(innerCtx, v)
				case v, ok := <-inner:
					if !ok {
						// Exhausted inner, disable this case.
						inner = nil
						continue
					}
					out <- v
				}
			}
		}()

		return out
	}
}

// Window maps the input to a series of emitters that just copy the input values.
// Every time emit emits a new inner emitter that copies the input values is emitted.
//
// In other words this is like buffer, but instead of emitting slices of values
// it emits emitters of these values.
//
// Consecutive emissions that emit no values do not cause empty emitters to be
// generated.
func Window[T, D any](emit <-chan D) Operator[T, <-chan T] {
	return func(in <-chan T) <-chan (<-chan T) {
		out := make(chan (<-chan T))
		go func() {
			defer close(out)

			inner := make(chan T)
			sent := false
			defer func() {
				if inner != nil {
					close(inner)
				}
			}()

			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					if !sent {
						out <- inner
						sent = true
					}
					inner <- v
				case _, ok := <-emit:
					if !ok {
						return
					}
					if !sent {
						continue
					}
					close(inner)
					inner = make(chan T)
					sent = false
				}
			}
		}()

		return out
	}
}

// WindowCount is like Window but it starts a new inner emitter every count emissions
// received from the input.
// The last inner emitter might emit less than count items.
func WindowCount[T any](count int) Operator[T, <-chan T] {
	return func(in <-chan T) <-chan (<-chan T) {
		out := make(chan (<-chan T))
		go func() {
			defer close(out)

			inner := make(chan T)
			defer func() {
				if inner != nil {
					close(inner)
				}
			}()
			c := 0
			for v := range in {
				if c == 0 {
					out <- inner
				}
				c++
				inner <- v
				if c >= count {
					c = 0
					close(inner)
					inner = make(chan T)
				}
			}
		}()

		return out
	}
}

/////////////////////////
// Filtering Operators //
/////////////////////////

// Audit can be used to observe the last emitted value for the input emitter.
// When emit emits, the most recent value for the input will be forwarded to the output.
//
// Note that this might cause repeated items to be emitted, for example if emit
// fires twice between input emissions.
// If this behavior is not desired, please use Sample instead.
func Audit[T, D any](emit <-chan D) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)

			var t T
			readOnce := false

			for {
				select {
				case _, ok := <-emit:
					if readOnce {
						out <- t
					}
					if !ok || in == nil {
						return
					}
				case v, ok := <-in:
					if !ok {
						return
					}
					t = v
					readOnce = true
				}
			}
		}()
		return out
	}
}

// Filter conditionally forwards inputs to the output.
// Values that the predicate returns true for are forwarded.
func Filter[T any](predicate func(T) bool) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for v := range in {
				ok := predicate(v)
				if !ok {
					continue
				}
				out <- v
			}
		}()
		return out
	}
}

// FilterCancel is like filter, but it can report when the last emission happens,
// which will cause the output channel to be closed and the input to be drained.
func FilterCancel[T any](predicate func(T) (emit, last bool), cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for v := range in {
				emit, last := predicate(v)
				if emit {
					out <- v
				}
				if last {
					drain(in, cancelParent)
					return
				}
			}
		}()
		return out
	}
}

// Distinct forwards the input to the output for values that have not been seen yet.
// Note: this needs to keep a set of all seen values in memory, so it might use
// a lot of memory.
func Distinct[T comparable]() Operator[T, T] {
	return func(in <-chan T) <-chan T {
		seen := map[T]bool{}
		return Filter(func(in T) bool {
			if seen[in] {
				return false
			}
			seen[in] = true
			return true
		})(in)
	}
}

// DistinctKey is like Distinct but it accepts a keyer function to compare elements.
func DistinctKey[T any, K comparable](keyer func(T any) K) Operator[T, T] {
	seen := map[K]bool{}
	return Filter(func(in T) bool {
		k := keyer(in)
		if seen[k] {
			return false
		}
		seen[k] = true
		return true
	})
}

// DistinctUntilChangedFunc forwards the input to the output, with the exception
// of identical consecutive values, which are discarded.
// In other words this behaves like slices.Compact, but for channels.
func DistinctUntilChangedFunc[T any](equals func(T, T) bool) Operator[T, T] {
	first := true
	var prev T
	return Filter(func(in T) bool {
		if first {
			first = false
			return true
		}
		tmp := prev
		prev = in
		return !equals(tmp, in)
	})
}

// DistinctUntilKeyChanged is like DistinctUntilChangedFunc but uses a key function to match
// consecutive values.
func DistinctUntilKeyChanged[T any, K comparable](key func(T) K) Operator[T, T] {
	return DistinctUntilChangedFunc(func(a, b T) bool {
		return key(a) == key(b)
	})
}

// DistinctUntilChanged is like DistinctUntilChangedFunc but it works on comparable items.
func DistinctUntilChanged[T comparable]() Operator[T, T] {
	return DistinctUntilChangedFunc(func(a, b T) bool {
		return a == b
	})
}

// DistinctUntilChangedEqualer is like DistinctUntilChangedFunc, but it compares
// items that have a Equal(T)bool method on them.
func DistinctUntilChangedEqualer[T interface{ Equal(T) bool }]() Operator[T, T] {
	return DistinctUntilChangedFunc(func(a, b T) bool {
		return a.Equal(b)
	})
}

// At emits the item at the provided index (if any) and concludes.
func At[T any](index int, cancelParent func()) Operator[T, T] {
	cur := 0
	return FilterCancel(func(in T) (emit, last bool) {
		if cur == index {
			return true, true
		}
		cur++
		return false, false
	}, cancelParent)
}

// AtWithDefault is like At but if the input concludes too early a default value
// is emitted.
func AtWithDefault[T any](index int, deflt T, cancelParent func()) Operator[T, T] {
	return Combine(
		At[T](index, cancelParent),
		DefaultIfEmpty(deflt),
	)
}

// Take forwards the first count items of the input to the output, then it ends.
func Take[T any](count int, cancelParent func()) Operator[T, T] {
	cur := 0
	return FilterCancel(func(in T) (emit, last bool) {
		if cur >= count {
			return false, true
		}
		cur++
		return true, false
	}, cancelParent)
}

// TakeUntil forwards the input to the output until the first emission (or closure)
// of emit, then it ends.
func TakeUntil[T, D any](emit <-chan D, cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for {
				select {
				case v, ok := <-in:
					if !ok {
						return
					}
					out <- v
				case <-emit:
					drain(in, cancelParent)
					return
				}
			}
		}()
		return out
	}
}

// First emits the first item that the predicate returns true for and exits.
// If predicate is nil, the first element from the input is forwarded to the output.
func First[T any](predicate func(T) bool, cancelParent func()) Operator[T, T] {
	if predicate == nil {
		predicate = func(T) bool { return true }
	}
	return FilterCancel(func(in T) (emit, last bool) {
		ok := predicate(in)
		return ok, ok
	}, cancelParent)
}

// IgnoreElements never emits, and it concludes when the input ends.
func IgnoreElements[D any]() Operator[D, D] {
	return Filter(func(D) bool { return false })
}

// Last only emits the last element from the input that the predicate retuned
// true for (if any) when the input is closed.
// If the predicate is nil, the last element is emitted.
func Last[T any](predicate func(T) bool) Operator[T, T] {
	if predicate == nil {
		predicate = func(T) bool { return true }
	}
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var matchedOnce bool
			var last T
			for v := range in {
				if !predicate(v) {
					continue
				}
				last = v
				matchedOnce = true
			}
			if matchedOnce {
				out <- last
			}
		}()
		return out
	}
}

// Sample emits the latest value from the input when the provided emitter emits.
// If no emission from the input has happened between consecutive requests to emit,
// they are ignored.
// In other words, no input elements are emitted more than once.
// If this behavior is not desired, please use Audit instead.
func Sample[T, D any](emit <-chan D) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)

			var t T
			read := false

			for {
				select {
				case _, ok := <-emit:
					if read {
						out <- t
						read = false
					}
					if !ok || in == nil {
						return
					}
				case v, ok := <-in:
					if !ok {
						return
					}
					t = v
					read = true
				}
			}
		}()
		return out
	}
}

// Skip discards the first count emissions from the input, then it becomes the
// copy of the input.
func Skip[T any](count int) Operator[T, T] {
	cur := 0
	return Filter(func(in T) bool {
		if cur < count {
			cur++
			return false
		}
		return true
	})
}

// SkipLast skips the last count items from the input.
// Since it needs to keep a buffer, SkipLast will wait until count items are emitted
// or the input is closed before emitting the first value, and then it becomes
// a delayed copy of the input that will not emit the last count items.
func SkipLast[T any](count int) Operator[T, T] {
	if count == 0 {
		// Instead of skipping 0 items we ust return the input
		return func(in <-chan T) <-chan T { return in }
	}
	buf := make(chan T, count)
	return MapFilter(func(in T) (T, bool) {
		if len(buf) < count {
			buf <- in
			return in, false
		}
		ret := <-buf
		buf <- in
		return ret, true
	})
}

////////////////////
// Join Operators //
////////////////////

// StartWith emits the initial value and then it becomes a copy of the input.
func StartWith[T any](initial T) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			out <- initial
			for v := range in {
				out <- v
			}
		}()
		return out
	}
}

// WithLatestFrom starts emitting when both the input and the other emitter have
// emitted at least once.
// It then emits every time the input emits, pairing that value with the latest
// coming from the other one.
// If the other emitter is closed, its last emitted value will still be used until
// the input is closed.
// If the other emitter never emits, the returned emitter will also never emit and
// end when the input ends.
func WithLatestFrom[I, L any](other <-chan L, cancelOther func()) Operator[I, Pair[I, L]] {
	return func(in <-chan I) <-chan Pair[I, L] {
		out := make(chan Pair[I, L])
		go func() {
			defer close(out)
			var cur Pair[I, L]
			otherEmitted := false
			for {
				select {
				case v, ok := <-in:
					if !ok {
						drain(other, cancelOther)
						return
					}
					if !otherEmitted {
						continue
					}
					cur.A = v
					out <- cur
				case v, ok := <-other:
					if !ok {
						// Disable this case.
						other = nil
						continue
					}
					otherEmitted = true
					cur.B = v
				}
			}
		}()
		return out
	}
}

///////////////////////
// Utility Operators //
///////////////////////

// Tap calls the observer for every value emitted by the input and forwards that
// value to the output once observer returns.
func Tap[T any](observer func(T)) Operator[T, T] {
	return Map(func(t T) T {
		observer(t)
		return t
	})
}

// Timeout creates a copy of the input unless the input takes more than maxDuration
// to emit the first value, in which case the output is closed.
func Timeout[T any](c Clock, maxDuration time.Duration, cancelParent func()) Operator[T, T] {
	if c == nil {
		c = NewRealClock()
	}
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			// TODO: use a timer factory, so that
			// we can stop the timer when the input emits
			expire := c.After(maxDuration)
			select {
			case v, ok := <-in:
				if !ok {
					return
				}
				out <- v
			case <-expire:
				drain(in, cancelParent)
				return
			}
			for v := range in {
				out <- v
			}
		}()
		return out
	}
}

// Teardown returns an emitter that is the copy of the input, but it calls
// deferred at the end of the input with the last emitted value, or with
// the zero value and emitted set to false.
// The deferred func can optionally return a value to emit as a last value.
func Teardown[T any](deferred func(last T, emitted bool) (additional T, emitAdditional bool)) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var emitted bool
			var last T
			defer func() {
				a, e := deferred(last, emitted)
				if !e {
					return
				}
				out <- a
			}()
			for v := range in {
				last = v
				emitted = true
				out <- v
			}
		}()
		return out
	}
}

///////////////////////////////////////
// Conditional and Boolean Operators //
///////////////////////////////////////

// DefaultIfEmpty emits the default value if and only if the input emitter did not
// emit before being closed.
func DefaultIfEmpty[T any](deflt T) Operator[T, T] {
	return Teardown(func(_ T, emitted bool) (T, bool) {
		return deflt, !emitted
	})
}

// Every returns whether all values emitted by the input match the predicate.
// If no value is emitted, it returns true.
// Every cancels and discard the parent as soon as the first non-matching value is encountered.
func Every[T any](predicate func(T) bool, cancelParent func()) Operator[T, bool] {
	return func(in <-chan T) <-chan bool {
		out := make(chan bool)
		go func() {
			defer close(out)
			for v := range in {
				if !predicate(v) {
					drain(in, cancelParent)
					out <- false
					return
				}
			}
			out <- true
		}()
		return out
	}
}

// FindIndex emits the index for the first input that matches the predicate and it ends.
func FindIndex[T any](predicate func(T) bool, cancelParent func()) Operator[T, int] {
	return func(in <-chan T) <-chan int {
		out := make(chan int)
		go func() {
			defer close(out)
			count := 0
			for v := range in {
				if !predicate(v) {
					count++
					continue
				}
				drain(in, cancelParent)
				out <- count
				return
			}
		}()
		return out
	}
}

// IsEmpty emits exactly once to report whether the input is empty or not.
func IsEmpty[D any](cancelParent func()) Operator[D, bool] {
	return func(in <-chan D) <-chan bool {
		out := make(chan bool)
		go func() {
			defer close(out)
			_, ok := <-in
			if ok {
				drain(in, cancelParent)
			}
			out <- !ok
		}()
		return out
	}
}

//////////////////////////////////////////
// Mathematical and Aggregate Operators //
//////////////////////////////////////////

func Reduce[I, O any](project func(accum O, in I) O, seed O) Operator[I, O] {

	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			acc := seed
			for v := range in {
				acc = project(acc, v)
			}
			out <- acc
		}()
		return out
	}
}

func ReduceAcc[I, O, A any](project func(accum A, in I) (newAccum A, out O), seed A) Operator[I, O] {
	acc := seed
	return Map(func(in I) (o O) {
		acc, o = project(acc, in)
		return o
	})
}

func ReducePreambleAcc[I, O, A any](preamble func(in I) (A, O), project func(accum A, in I) (A, O)) Operator[I, O] {
	var accum A
	emitted := false
	return ReduceAcc(func(accum A, in I) (A, O) {
		if !emitted {
			emitted = true
			return preamble(in)
		}
		return project(accum, in)
	}, accum)
}
func ReducePreamble[I, O any](preamble func(in I) O, project func(O, I) O) Operator[I, O] {
	return ReducePreambleAcc(
		func(in I) (O, O) {
			v := preamble(in)
			return v, v
		},
		func(acc O, in I) (O, O) {
			o := project(acc, in)
			return o, o
		},
	)
}

func Count[D any]() Operator[D, int] {
	return Combine(
		Reduce(func(c int, _ D) int { return c + 1 }, 0),
		Last[int](nil),
	)
}

func Max[T constraints.Ordered]() Operator[T, T] {
	return Combine(
		ReducePreamble(
			func(i T) T { return i },
			func(max, i T) T {
				if max < i {
					return i
				}
				return max
			}),
		Last[T](nil),
	)
}

func Min[T constraints.Ordered]() Operator[T, T] {
	return Combine(
		ReducePreamble(
			func(i T) T { return i },
			func(min, i T) T {
				if min > i {
					return i
				}
				return min
			}),
		Last[T](nil),
	)
}

/*
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)

		}()
		return out
	}
*/

/////////////////////////
// Consuming Operators //
/////////////////////////

func Discard[T any]() SinkOperator[T] {
	return func(in <-chan T) {
		if in == nil {
			return
		}
		go func() {
			for range in {
			}
		}()
	}
}

func ToSlice[T any](in <-chan T) []T {
	var res []T
	for v := range in {
		res = append(res, v)
	}
	return res
}

func ToSliceParallel[T any](in <-chan T) <-chan []T {
	out := make(chan []T)
	go func() {
		defer close(out)
		var res []T
		for v := range in {
			res = append(res, v)
		}
		out <- res
	}()
	return out
}

func ToSlices[I, L any](i <-chan I, l <-chan L) ([]I, []L) {
	var ri []I
	var rl []L
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for vi := range i {
			ri = append(ri, vi)
		}
	}()
	go func() {
		defer wg.Done()
		for vl := range l {
			rl = append(rl, vl)
		}
	}()
	wg.Wait()
	return ri, rl
}

func Collect[T any](consume func(T) (ok bool), cancelParent func()) SinkOperator[T] {
	return func(in <-chan T) {
		for v := range in {
			if ok := consume(v); !ok {
				if cancelParent != nil {
					cancelParent()
				}
				// TODO: point out why [T] is needed
				Discard[T]()(in)
			}
		}
	}
}

///////////////////////////
// Hiher-order Operators //
///////////////////////////

// Combine returns an operator that is the combination of the two input ones.
// When using Combine keep in mind that every operator usually spawns at least one
// goroutine, so very long combination chains might end up being expensive to run.
//
// Also consider that re-using operators might cause unexpected behaviors as some
// operators preserve state, so for these cases consider CombineFactories or re-calling
// Combine with freshly built operators every time the combined result is needed.
func Combine[I, T, O any](a Operator[I, T], b Operator[T, O]) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		t := a(in)
		return b(t)
	}
}

// CombineFactories is like Combine, but it works on operator constructors instead of
// operators.
func CombineFactories[I, T, O any](fa func() Operator[I, T], fb func() Operator[T, O]) func() Operator[I, O] {
	return func() Operator[I, O] {
		a := fa()
		b := fb()
		return func(in <-chan I) <-chan O {
			t := a(in)
			return b(t)
		}
	}
}

///////////
// Utils //
///////////

func drain[T any](in <-chan T, cancel func()) {
	if cancel != nil {
		cancel()
	}
	Discard[T]()(in)
}
