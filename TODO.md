# Creation Operators
* [x] from
* [x] generate
* [x] interval
* [x] range

Join Creation Operators

* [x] combineLatest
* [x] concat
* [x] forkJoin
* [x] merge
* [x] partition
* [x] race
* [x] zip

Transformation Operators

* [x] buffer
* [x] bufferCount
* [x] bufferTime
* [x] bufferToggle
* [x] concatMap
* [x] exhaustMap
* [x] map
* [x] mergeMap
* [x] pairwise
* [x] scan
* [x] switchMap
* [x] window
* [x] windowCount

Filtering Operators

* [x] audit
* [ ] debounce
* [ ] debounceTime
* [x] distinct
* [x] distinctUntilChanged
* [ ] distinctUntilKeyChanged // get a func key(T)comparable ?
* [x] elementAt
* [x] filter
* [x] first
* [x] ignoreElements
* [x] last
* [x] sample
* [ ] sampleTime
* [x] skip
* [x] skipLast
* [ ] skipUntil
* [ ] skipWhile
* [x] take
* [ ] takeLast
* [ ] takeUntil
* [ ] takeWhile
* [ ] throttle
* [ ] throttleTime

Join Operators

* [ ] concatAll
* [ ] exhaustAll
* [ ] mergeAll
* [ ] switchAll
* [x] startWith
* [x] withLatestFrom

Utility Operators

* [x] tap
* [x] delay
* [x] delayWhen
* [x] timeout
* [ ] timeoutWith

Conditional and Boolean Operators

* [x] defaultIfEmpty
* [x] every
* [x] findIndex
* [x] isEmpty

Mathematical and Aggregate Operators

* [x] count
* [x] max
* [x] min
* [x] reduce

Moar
* [ ] TakeFunc
* [ ] ApplyAll([]<-chan, operator)[]<-chan
* [ ] FanOut(<-chan)[]<-chan
* [ ] FanIn([]<-chan)<-chan
* [ ] BufferChan
* [ ] Tee
* [ ] Clone
* [ ] TapAccum
* [ ] WithTearDown (accepts a cleanup func)
* [ ] SampleN
* [ ] SamplePercent
* [ ] ParallelMap
* [ ] ParallelMapStable
* [ ] Multicast
* [ ] ScanAccum
* [ ] Lossy (discard when you can't send)
* [ ] CollectFirst(<-chan T)T
* [ ] WithTearDown
* [ ] LimitedSuccesses