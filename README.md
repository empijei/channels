# Raggio (/ˈradʤo/)

Yet another Go channels library.

**DISCLAIMER**: This is not an officially supported Google product.

No, really, this is not.

This is code that *happens* to be owned by Google for legal reasons.

This doesn't even follow code readability good practices and it wastes a massive
amount of resources.

It's a toy project, please don't use.

# But what is this, really

This is my take on channel operator functions.

I tried to re-implement [RxJS](https://rxjs.dev/guide/operators) in Go, but
instead of using observables and propagate change immediately, I wrote the
operators to all work in parallel.

This works basically like a bash pipeline, with channels as the pipes and funcs
as programs.

This is, of course, super wasteful as every operator creates at least one goroutine,
but I wanted to experiment with the concept of reactive Go.
