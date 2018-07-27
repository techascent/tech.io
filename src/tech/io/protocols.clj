(ns tech.io.protocols
  "Protocols used.  In separate namespace to make development using repl a bit simpler")


(defprotocol IOProvider
  "Base stream and resource abstraction"
  (input-stream [provider url-parts options])
  (output-stream! [provider url-parts options])
  (exists? [provider url-parts options])
  (ls [provider url-parts options]
    "Returns a possibly lazy sequence of maps where each map contains at least
{:url}.

Defaults to non recursive, pass in recursive? true in options if recursive behavior is
desired.")
  (delete! [provider url-parts options]
    "Recursive delete.  No option for non recursive as would require an ls type check in general.")
  (metadata [provider url-parts options]
    "At least modify date.  Potentially byte-length.
:modify-date (required) (Date.)
:byte-length (optional) long"))


(defprotocol ICopyObject
  "Interface that allows optimized implementations assuming you want to copy an thing.
Default implementation is provided so it is optional from a provider's perspective."
  (get-object [provider url-parts options]
    "Get an object that is either an input stream or convertible to
an input stream.  For example, if you are using file-caching then
this will return a file that points to the cached data.  This allows easier
usage of 3rd party API's that must take files.")
  (put-object! [provider url-parts value options]
    "Putting an object gives the underlying provider more information than does requesting
a stream.  It may be significantly more efficient, for example, to put-object! a file rather than
io/copy the FileInputStream specifically if using amazon's s3 system."))


(defmulti url-parts->provider
  "Static conversion of a protocol to a provider."
  (fn [url-parts]
    (:protocol url-parts)))


(defprotocol IUrlRedirect
  "Testing interface"
  (url->redirect-url [provider url]))
