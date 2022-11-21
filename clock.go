package channels

import "time"

// Clock is an interface to abstract over the time package.
type Clock interface {
	After(time.Duration) <-chan time.Time
}

type RealClock struct{}

func NewRealClock() RealClock {
	return RealClock{}
}

func (RealClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

// Ticker is an interface to abstract over a time.Ticker.
type Ticker interface {
	Stop()
	Chan() <-chan time.Time
}

type RealTicker struct {
	t *time.Ticker
}

func (r RealTicker) Stop() {
	r.t.Stop()
}
func (r RealTicker) Chan() <-chan time.Time {
	return r.t.C
}

func NewRealTicker(d time.Duration) Ticker {
	return RealTicker{time.NewTicker(d)}
}
