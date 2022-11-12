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

import "time"

// This file is a stripped-down version for the real clock implementation
// of clockwork.

// Ticker provides an interface which can be used instead of directly
// using the ticker within the time module.
type Ticker interface {
	Chan() <-chan time.Time
	Stop()
}

var _ Ticker = realTicker{}

type realTicker struct{ *time.Ticker }

func (rt realTicker) Chan() <-chan time.Time {
	return rt.C
}

// Clock is an interface that this package consumes instead of directly
// using the time module, so that chronology-related behavior can be tested
type Clock interface {
	After(d time.Duration) <-chan time.Time
	Sleep(d time.Duration)
	NewTicker(d time.Duration) Ticker
}

var _ Clock = realClock{}

type realClock struct{}

func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

func (realClock) Sleep(d time.Duration) { time.Sleep(d) }

func (realClock) NewTicker(d time.Duration) Ticker {
	return realTicker{time.NewTicker(d)}
}
