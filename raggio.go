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

TODO: potentially create or require cancel funcs for inner stuff that might be discarded
and cancel them once they get discarded (e.g. for switchmap)

TODO: check for if statements for first or similar conditions: can we set the condition inside the if?

TODO: check all outs are closed.

TODO: document that last values are kept and emitted whenever possible, or change code to not do it.

TODO: check if I forgot some discards

TODO: check that variables we close over are necessary
*/

////////////////////////
// Creation Operators //
////////////////////////

func FromFunc[T any](generator func(index int) (t T, ok bool)) <-chan T {
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

func FromSlice[T any](s []T) <-chan T {
	out := make(chan T)
	go func() {
		defer close(out)
		for _, v := range s {
			out <- v
		}
	}()
	return out
}

func FromMap[M ~map[K]V, K comparable, V any](m M) <-chan struct {
	k K
	v V
} {
	out := make(chan struct {
		k K
		v V
	})
	go func() {
		defer close(out)
		for k, v := range m {
			out <- struct {
				k K
				v V
			}{k, v}
		}
	}()
	return out
}

func Ticker(duration time.Duration, max int) <-chan time.Time {
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

func Range(start, end int) <-chan int {
	out := make(chan int)
	go func() {
		defer close(out)
		for i := start; i < end; i++ {
			out <- i
		}
	}()
	return out
}

/////////////////////////////
// Join Creation Operators //
/////////////////////////////

func CombineLatest[A, B any](a <-chan A, b <-chan B) <-chan struct {
	a A
	b B
} {

	out := make(chan struct {
		a A
		b B
	})

	go func() {
		defer close(out)

		var (
			outS struct {
				a A
				b B
			}
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
				outS.a = gotA
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
				outS.b = gotB
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

func Concat[T any](chans ...<-chan T) <-chan T {
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

func Merge[T any](chans ...<-chan T) <-chan T {
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

func Partition[T any](in <-chan T, condition func(t T) bool) (then, elze <-chan T) {
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

func Race[T any](chans ...<-chan T) <-chan T {
	out := make(chan T)

	var firstOnce sync.Once
	arrivedFirst := func() bool {
		isFirst := false
		firstOnce.Do(func() { isFirst = true })
		return isFirst
	}

	for _, c := range chans {
		c := c
		go func() {
			firstIteration := true
			for v := range c {
				if firstIteration {
					firstIteration = false
					if !arrivedFirst() {
						// We lost the race, discard input and return.
						Discard(c)
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

func Zip[A, B any](a <-chan A, b <-chan B) <-chan struct {
	a A
	b B
} {
	out := make(chan struct {
		a A
		b B
	})

	go func() {
		defer close(out)

		var outS struct {
			a A
			b B
		}
		inA := a
		inB := b
		for {
			// The two cases for this select are identical and symmetric.
			select {
			case gotA, ok := <-inA:
				if !ok {
					// chan A was closed, let's just consume B and end.
					Discard(b)
					return
				}
				// Store the received value
				outS.a = gotA
				if inB == nil {
					// chan A emitted second for this pair, let's emit and re-enable both cases.
					out <- outS // This send copies the current value for outS.
					outS = struct {
						a A
						b B
					}{}
					inB = b
					continue
				}
				// chan A emitted first for this pair. Let's disable this case to wait
				// for B to emit.
				inA = nil
			case gotB, ok := <-inB:
				if !ok {
					// chan B was closed, let's just consume A and end.
					Discard(a)
					return
				}
				// Store the received value
				outS.b = gotB
				if inA == nil {
					// chan B emitted second for this pair, let's emit and re-enable both cases.
					out <- outS // This send copies the current value for outS.
					outS = struct {
						a A
						b B
					}{}
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

//////////////////////////////
// Transformation Operators //
//////////////////////////////

func Buffer[T, I any](in <-chan T, emit <-chan I) <-chan []T {
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

func BufferCount[T any](in <-chan T, count int) <-chan []T {
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

func BufferTime[T any](in <-chan T, duration time.Duration) <-chan []T {
	t := time.NewTicker(duration)
	return Buffer(in, t.C)
}

func BufferToggle[T, I1, I2 any](in <-chan T, openings <-chan I1, closings <-chan I2) <-chan []T {
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

func ConcatMap[IN, OUT any](in <-chan IN, project func(in IN) <-chan OUT) <-chan OUT {
	out := make(chan OUT)

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

func ExhaustMap[IN, OUT any](in <-chan IN, project func(in IN) <-chan OUT) <-chan OUT {
	out := make(chan OUT)

	go func() {
		defer close(out)
		var inner <-chan OUT
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

func Map[IN, OUT any](in <-chan IN, project func(in IN) OUT) <-chan OUT {
	out := make(chan OUT)
	go func() {
		defer close(out)
		for v := range in {
			out <- project(v)
		}
	}()
	return out
}

func MergeMap[IN, OUT any](in <-chan IN, project func(in IN) <-chan OUT) <-chan OUT {
	out := make(chan OUT)

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

func PairWise[T any](in <-chan T) <-chan [2]T {
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

func Scan[IN, OUT any](in <-chan IN, project func(accum OUT, cur IN) OUT, seed OUT) <-chan OUT {
	accum := seed
	return Map(in, func(i IN) OUT {
		accum = project(accum, i)
		return accum
	})
}

func SwitchMap[IN, OUT any](in <-chan IN, project func(in IN) <-chan OUT) <-chan OUT {
	out := make(chan OUT)

	go func() {
		defer close(out)
		var inner <-chan OUT
		for {
			select {
			case v, ok := <-in:
				if inner != nil {
					// Ignore inner values in favor of fresher ones.
					Discard(inner)
				}
				if !ok {
					// Abort everything.
					return
				}
				inner = project(v)
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

func Window[T, I any](in <-chan T, emit <-chan I) <-chan (<-chan T) {
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

/////////////////////////
// Filtering Operators //
/////////////////////////

func Audit[T, I any](in <-chan T, emit <-chan I) <-chan T {
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

func Filter[T any](in <-chan T, predicate func(T) bool) <-chan T {
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

func Distinct[T comparable](in <-chan T) <-chan T {
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

func DistinctUntilChanged[T comparable](in <-chan T) <-chan T {
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

func DistinctUntilChangedEqualer[T interface{ Equals(T) bool }](in <-chan T) <-chan T {
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

func At[T any](in <-chan T, index int) <-chan T {
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
			Discard(in)
			return
		}
	}()
	return out
}

func AtWithDefault[T any](in <-chan T, index int, deflt T) <-chan T {
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
			Discard(in)
			return
		}
		out <- deflt
	}()
	return out
}

func First[T any](in <-chan T, predicate func(T) bool) <-chan T {
	if predicate == nil {
		predicate = func(T) bool { return true }
	}
	flt := Filter(in, predicate)
	return Take(flt, 1)
}

func Take[T any](in <-chan T, count int) <-chan T {
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
				Discard(in)
				return
			}
			out <- v
			cur++
		}
	}()
	return out
}

func IgnoreElements[T any](in <-chan T) <-chan struct{} {
	out := make(chan struct{})
	go func() {
		defer close(out)
		for range in {
		}
	}()
	return out
}

func Last[T any](in <-chan T) <-chan T {
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

func Sample[T, I any](in <-chan T, emit <-chan I) <-chan T {
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

func Skip[T any](in <-chan T, count int) <-chan T {
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

func SkipLast[T any](in <-chan T, count int) <-chan T {
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

////////////////////
// Join Operators //
////////////////////

func StartWith[T any](in <-chan T, initial T) <-chan T {
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

/*

	out := make(chan T)
	go func() {
		defer close(out)

	}()
	return out
*/

/////////////////////////
// Consuming Operators //
/////////////////////////

func Discard[T any](in <-chan T) {
	go func() {
		for range in {
		}
	}()
}

func Collect[T any](in <-chan T) []T {
	var res []T
	for v := range in {
		res = append(res, v)
	}
	return res
}

func ToSlice[T any](in <-chan T, consume func(T) (ok bool)) {
	for v := range in {
		if ok := consume(v); !ok {
			Discard(in)
		}
	}
}
