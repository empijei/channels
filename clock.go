package raggio

import "time"

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
