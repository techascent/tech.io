(ns tech.v3.io.common)

(defn args->map
  [args]
  (if (map? args)
    args
    (apply hash-map args)))

