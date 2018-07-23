(ns tech.io.protocols
  "Protocols used.  In separate namespace to make development using repl a bit simpler")


(defprotocol IOProvider
  "Base stream and"
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
  (meta-data [provider url-parts options]
    "At least modify date if anything:
{:modify-date (Date.)"))


(defmulti url-parts->provider
  "Static conversion of a protocol to a provider."
  (fn [url-parts]
    (:protocol url-parts)))


(defprotocol IUrlCache
  (url->cache-url [provider url-parts options]))
