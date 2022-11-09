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

package raggio

import (
	"context"
	"sync"
	"time"
)

/*
TODO: point this out
type declarations inside generic functions are not currently supported

type outT = struct {
a A
b B
}

TODO: potentially create or require contexts for inner stuff that might be discarded
and cancel them once they get discarded (e.g. for switchmap)

TODO: check for if statements for first or similar conditions: can we set the condition inside the if?

TODO: check all outs are closed.

TODO: document that last values are kept and emitted whenever possible, or change code to not do it.

TODO: check if I forgot some discards

TODO: check that variables we close over are necessary

TODO: find a way to return contextx when spawning subroutines the caller does not control

TODO: chech that types for emit are D instead of other letters

TODO: point out
  // error: generic type cannot be alias
	// MonoTypeOperator[T any] = Operator[T, T]

TODO: express operators in terms of other operators.

TODO: make time related operators get a clock in input
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

	FanInOperator[I, O any] func([]<-chan I) <-chan O

	ParallelOperator[I, O any] func([]<-chan I) []<-chan O

	Pair[A, B any] struct {
		A A
		B B
	}
)

////////////////////////
// Creation Operators //
////////////////////////

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

func Ticker(duration time.Duration, max int) SourceOperator[time.Time] {
	return func() <-chan time.Time {
		out := make(chan time.Time)
		go func() {
			defer close(out)
			t := time.NewTicker(duration)
			defer t.Stop()
			for i := 0; i < max; i++ {
				now := <-t.C
				out <- now
			}
		}()
		return out
	}
}

func Range(start, end int) SourceOperator[int] {
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

func Concat[T any]() FanInOperator[T, T] {
	return func(chans []<-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			for _, c := range chans {
				c := c
				for v := range c {
					out <- v
				}
			}
		}()
		return out
	}
}

func Merge[T any]() FanInOperator[T, T] {
	return func(chans []<-chan T) <-chan T {
		out := make(chan T)

		var wg sync.WaitGroup
		wg.Add(len(chans))
		go func() {
			defer close(out)
			wg.Wait()
		}()

		for _, c := range chans {
			c := c
			go func() {
				defer wg.Done()
				for v := range c {
					out <- v
				}
			}()
		}

		return out
	}
}

func Partition[T any](condition func(t T) bool) PartitionOperator[T, T, T] {
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

func Race[T any](cancels ...func()) FanInOperator[T, T] {
	return func(chans []<-chan T) <-chan T {
		out := make(chan T)

		var firstOnce sync.Once
		arrivedFirst := func() bool {
			isFirst := false
			firstOnce.Do(func() { isFirst = true })
			return isFirst
		}

		for i, c := range chans {
			i := i
			c := c
			go func() {
				firstIteration := true
				for v := range c {
					if firstIteration {
						firstIteration = false
						if !arrivedFirst() {
							// We lost the race, discard input and return.
							var canc func()
							if len(cancels) > i {
								canc = cancels[i]
							}
							drain(c, canc)
							return
						}
						// We won the race, we are responsible to close the out chan once we are done.
						defer close(out)
					}

					// We won the race, we are responsible to write to the out chan.
					out <- v
				}
			}()
		}
		return out
	}
}

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

func Buffer[T, D any](emit <-chan D) Operator[T, []T] {
	// TODO: how to deal with the fact that we can't use multipe buffers with the
	// same emit chan?
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
						// Emitter is closed, exit.
						return
					}
				}
			}
		}()

		return out
	}
}

func BufferCount[T any](count int) Operator[T, []T] {
	return func(in <-chan T) <-chan []T {
		if count <= 0 {
			count = 1
		}

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

			for v := range in {
				buf = append(buf, v)
				if len(buf) >= count {
					emitBuf()
				}
			}
			emitBuf()
		}()

		return out
	}
}

func BufferTime[T any](duration time.Duration) Operator[T, []T] {
	return func(in <-chan T) <-chan []T {
		t := time.NewTicker(duration)
		emit := t.C
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
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		go func() {
			defer close(out)

			for v := range in {
				inner := project(v)
				for v := range inner {
					out <- v
				}
			}
		}()

		return out
	}
}

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

func MapCancel[I, O any](project func(in I) (projected O, ok bool), cancelParent func()) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)
		go func() {
			defer close(out)
			for v := range in {
				t, ok := project(v)
				if !ok {
					drain(in, cancelParent)
					return
				}
				out <- t
			}
		}()
		return out
	}
}

func MergeMap[I, O any](project func(in I) <-chan O) Operator[I, O] {
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
				inner := project(v)
				wg.Add(1)
				go func() {
					defer wg.Done()
					for v := range inner {
						out <- v
					}
				}()
			}
		}()

		return out
	}
}

func PairWise[T any]() Operator[T, [2]T] {
	return func(in <-chan T) <-chan [2]T {
		out := make(chan [2]T)

		go func() {
			defer close(out)

			var buf [2]T
			cur := 0
			for v := range in {
				buf[cur] = v
				cur++
				if cur < 2 {
					continue
				}
				out <- buf
				cur = 0
			}
		}()

		return out
	}
}

func Scan[I, O any](project func(accum O, cur I) O, seed O) Operator[I, O] {
	accum := seed
	return Map(func(i I) O {
		accum = project(accum, i)
		return accum
	})
}

func ScanAccum[I, O, A any](project func(accum A, cur I) (nextAccum A, o O), seed A) Operator[I, O] {
	accum := seed
	return Map(func(i I) O {
		nextAccum, out := project(accum, i)
		accum = nextAccum
		return out
	})
}

func SwitchMap[I, O any](ctx context.Context, project func(ctx context.Context, in I) <-chan O) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		out := make(chan O)

		go func() {
			defer close(out)
			var inner <-chan O
			innerCtx, innerCancel := context.WithCancel(ctx)
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

func Window[T, D any](emit <-chan D) Operator[T, <-chan T] {
	return func(in <-chan T) <-chan (<-chan T) {
		out := make(chan (<-chan T))
		go func() {
			defer close(out)

			inner := make(chan T)
			sent := false

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
					sent = false
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
						// Disable this case
						in = nil
						continue
					}
					t = v
					readOnce = true
				}
			}
		}()
		return out
	}
}

func Filter[T any](predicate func(T) bool) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)

			for v := range in {
				if !predicate(v) {
					continue
				}
				out <- v
			}
		}()
		return out
	}
}

func Distinct[T comparable]() Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			seen := map[T]bool{}
			for v := range in {
				if seen[v] {
					continue
				}
				seen[v] = true
				out <- v
			}
		}()
		return out
	}
}

func DistinctKeyer[T any, K comparable](keyer func(T) K) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			seen := map[K]bool{}
			for v := range in {
				k := keyer(v)
				if seen[k] {
					continue
				}
				seen[k] = true
				out <- v
			}
		}()
		return out
	}
}

func DistinctUntilChanged[T comparable]() Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			first := true
			var prev T
			for v := range in {
				if first {
					first = false
					prev = v
					out <- v
					continue
				}
				if prev == v {
					continue
				}
				out <- v
			}
		}()
		return out
	}
}

func DistinctUntilChangedEqualer[T interface{ Equals(T) bool }]() Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			first := true
			var prev T
			for v := range in {
				if first {
					first = false
					prev = v
					out <- v
					continue
				}
				if prev.Equals(v) {
					continue
				}
				out <- v
			}
		}()
		return out
	}
}

func At[T any](index int, cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			cur := 0
			for v := range in {
				if cur != index {
					cur++
					continue
				}
				out <- v
				drain(in, cancelParent)
				return
			}
		}()
		return out
	}
}

func AtWithDefault[T any](index int, deflt T, cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			cur := 0
			for v := range in {
				if cur != index {
					cur++
					continue
				}
				out <- v
				drain(in, cancelParent)
				return
			}
			out <- deflt
		}()
		return out
	}
}

func Take[T any](count int, cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		if count == 0 {
			close(out)
			return out
		}
		go func() {
			defer close(out)
			cur := 0
			for v := range in {
				if cur >= count {
					drain(in, cancelParent)
					return
				}
				out <- v
				cur++
			}
		}()
		return out
	}
}

func First[T any](predicate func(T) bool, cancelParent func()) Operator[T, T] {
	if predicate == nil {
		predicate = func(T) bool { return true }
	}
	// TODO: why are types needed here?
	return Combine[T, T, T](Filter[T](predicate), Take[T](1, cancelParent))
}

func IgnoreElements[D any]() Operator[D, struct{}] {
	return func(in <-chan D) <-chan struct{} {
		out := make(chan struct{})
		go func() {
			defer close(out)
			for range in {
			}
		}()
		return out
	}
}

func Last[T any]() Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			var t T
			read := false
			for v := range in {
				read = true
				t = v
			}
			if read {
				out <- t
			}
		}()
		return out
	}
}

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
						// Disable this case
						in = nil
						continue
					}
					t = v
					read = true
				}
			}
		}()
		return out
	}
}

func Skip[T any](count int) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			cur := 0
			for v := range in {
				if cur < count {
					cur++
					continue
				}
				out <- v
			}

		}()
		return out
	}
}

func SkipLast[T any](count int) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			buf := make(chan T, count)
			for v := range in {
				if len(buf) >= count {
					oldest := <-buf
					out <- oldest
				}
				buf <- v
			}
		}()
		return out
	}
}

////////////////////
// Join Operators //
////////////////////

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

func WithLatestFrom[I, L any](other <-chan L, cancelOther, cancelParent func()) Operator[I, Pair[I, L]] {
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
						drain(in, cancelParent)
						return
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

func Tap[T any](observer func(T)) Operator[T, T] {
	return Map[T, T](func(t T) T {
		observer(t)
		return t
	})
}

func Delay[T any](duration time.Duration) Operator[T, T] {
	return Tap[T](func(T) { time.Sleep(duration) })
}

func DelayWhen[T, D any](when <-chan D) Operator[T, T] {
	return Tap[T](func(T) { <-when })
}

func Timeout[T any](duration time.Duration, cancelParent func()) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			expire := time.After(duration)
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

/*
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)

		}()
		return out
	}
*/

///////////////////////////////////////
// Conditional and Boolean Operators //
///////////////////////////////////////

func DefaultIfEmpty[T any](deflt T) Operator[T, T] {
	return func(in <-chan T) <-chan T {
		out := make(chan T)
		go func() {
			defer close(out)
			emitted := false
			for v := range in {
				emitted = true
				out <- v
			}
			if !emitted {
				out <- deflt
			}
		}()
		return out
	}
}

func Every[T any](predicate func(T) bool, cancelParent func()) Operator[T, bool] {
	return func(in <-chan T) <-chan bool {
		out := make(chan bool)
		go func() {
			defer close(out)
			for v := range in {
				if ok := predicate(v); ok {
					continue
				}
				drain(in, cancelParent)
				out <- false
				return
			}
			out <- true
		}()
		return out
	}
}

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

func Combine[I, T, O any](a Operator[I, T], b Operator[T, O]) Operator[I, O] {
	return func(in <-chan I) <-chan O {
		t := a(in)
		return b(t)
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