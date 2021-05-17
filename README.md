# tech.io

[![Clojars Project](https://clojars.org/techascent/tech.io/latest-version.svg)](https://clojars.org/techascent/tech.io)

Simple Clojure IO library to enable rapid development on at least files, http/https, and s3.

* [API Documentation](https://techascent.github.io/tech.io/)


## Design

The philosophy is to split the difference between do-as-I-want programming which is what
I do at the repl and do-as-I-say programming which matches system programming much
closer.  "tech.io" is DWIW, but it is built on an set of protocols to enable very
precise DWIS programming where surprises are minimal.

### Do What I Want:
```clojure
(io/get-nippy "s3://blah/blah.nippy")

(io/copy "s3://blah/blah.nippy" "file://blah.nippy")

(io/put-image! "s3://blah/blah.jpg" buffered-image)

```
This layer has a global variable you can use to override the way the system maps from
url->io-provider.  Using this variable you can setup global caching so that anything
downloaded or read will get cached, regardless of where it came from (http, https, file,
s3).  You can also setup redirection where a directory looks like s3 for unit tests that
read/write to s3 urls.

## License

Copyright © 2018 TechAscent, LLC

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
