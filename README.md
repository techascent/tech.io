# tech.io

[![Clojars Project](https://clojars.org/techascent/tech.io/latest-version.svg)](https://clojars.org/techascent/tech.io)

Simple IO library to enable rapid development on at least files, http/https, s3 with optional caching.


## Design

The philosophy is to split the difference between do-as-I-want programming which is what I do at the repl and do-as-I-say programming which matches system programming much closer.  "tech.io" is DWIW, but it is built on an set of protocols to enable very precise DWIS programming where surprises are minimal.

### Do What I want:
```clojure
(io/get-nippy "s3://blah/blah.nippy")

(io/copy "s3://blah/blah.nippy" "file://blah.nippy") 

(io/put-image! "s3://blah/blah.jpg" buffered-image)

```
This layer has a global variable you can use to override the way the system maps from url->io-provider.    Using this variable you can setup global caching so that anything downloaded or read will get cached, regardless of where it came from (http, https, file, s3).  You can also setup redirection where a directory looks like s3 for unit tests that read/write to s3 urls.  

### Do What I Say

The intention is to provide a simple and minimal base io abstraction and then add capabilities via chaining
and layering abstractions.

The base abstraction is the [IOProvider](src/tech/io/protocols.clj).  There are basic implementations
of this abstraction in [base.clj](src/tech/io/base.clj).  An [aws s3](src/tech/io/s3.clj) layer is also provided.

Given this abstraction we can implement a [caching](src/tech/io/cache.clj) layer which will cache anything to a defined point
in the filesystem.  We can also implement a [redirection](src/tech/io/redirect.clj) layer which will redirect requests to a
defined point on the filesystem.

## Examples

Please see the [tests](test/tech/io_test.clj).

## License

Copyright © 2018 TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
