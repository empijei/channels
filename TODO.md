# Creation Operators
* [x] from
* [x] generate
* [x] interval
* [x] range

Join Creation Operators

* [x] combineLatest
* [x] concat
* [ ] forkJoin
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
* [ ] expand
* [x] map
* [x] mergeMap
* [ ] mergeScan
* [x] pairwise
* [x] scan
* [ ] switchScan
* [x] switchMap
* [x] window
* [ ] windowCount
* [ ] windowTime
* [ ] windowToggle
* [ ] windowWhen

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
* [ ] withLatestFrom

Utility Operators

* [ ] tap
* [ ] delay
* [ ] delayWhen
* [ ] timeout
* [ ] timeoutWith

Conditional and Boolean Operators

* [ ] defaultIfEmpty
* [ ] every
* [ ] find // Filter+Take(1)?
* [ ] findIndex // At???
* [ ] isEmpty // This should be a consumer

Mathematical and Aggregate Operators

* [ ] count
* [ ] max
* [ ] min
* [ ] reduce

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
