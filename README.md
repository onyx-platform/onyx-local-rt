# onyx-local-rt

An alternative runtime for Onyx. Executes jobes in a pure, deterministic environment.

## Usage

```clojure

(clojure.pprint/pprint
 (-> (init {:workflow [[:in :inc] [:inc :out]]
            :catalog [{:onyx/name :in
                       :onyx/type :input}
                      {:onyx/name :inc
                       :onyx/type :function
                       :onyx/fn ::my-inc}
                      {:onyx/name :out
                       :onyx/type :output}]
            :lifecycles []})
     (new-segment :in {:n 41})
     (new-segment :in {:n 84})
     (drain)
     (stop)
     (env-summary)))
```

## License

Copyright © 2016 Distributed Masonry

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
