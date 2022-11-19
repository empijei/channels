# Channels

Yet another Go channels library.

**DISCLAIMER**: This is not an officially supported Google product.

No, really, this is not.

This is code that *happens* to be owned by Google for legal reasons.

# But what is this, really

This is my take on channel operator functions.

I tried to re-implement [RxJS](https://rxjs.dev/guide/operators) in Go, but
instead of using observables and propagate change immediately, I wrote the
operators to all work in parallel.

This works basically like a bash pipeline, with channels as the pipes and funcs
as programs.

Note that every operator creates at least one goroutine, so it might not be the most
efficient solution out there, but I wanted to experiment with the concept.

We have `slices` and `maps` packages in the stdlib, seemed appropriate to try an
make a `channels` one.