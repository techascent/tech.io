# tech.io

[![Clojars Project](https://clojars.org/techascent/tech.io/latest-version.svg)](https://clojars.org/techascent/tech.io)

Simple IO library to enable rapid development on at least files, http/https, s3 with optional caching.


## Design

The intention is to provide a simple and minimal base io abstraction and then add capabilities via chaining
and layering abstractions.

The base abstraction is the [IOProvider](src/tech/io/protocols.clj).  There are basic implementations
of this abstraction in [base.clj](src/tech/io/base.clj).  An [aws s3](src/tech/io/s3.clj) layer is also provided.

Given this abstraction we can implement a [caching](src/tech/io/cache.clj) layer which will cache anything to a defined point
in the filesystem.  We can also implement a [redirection](src/tech/io/redirect.clj) layer which will redirect requests to a
defined point on the filesystem.

A minimal [layer](src/tech/io.clj) is provided for clojure.java.io interoperability and for ease of initial use.

## Examples

Please see the [tests](test/tech/io_test.clj).

## License

Copyright © 2018 TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
