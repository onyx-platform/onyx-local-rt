# onyx-local-rt

An alternative runtime for Onyx. Executes jobs in a pure, deterministic environment. This is an incubating repository. This
code will be moved into Onyx core at a later day.

## Goals

- Offer a subset of the Distributed Runtime functionality in an easier-to-configure environment.
- Target ClojureScript as an underlying execution environment.
- Guarantee ordering.
- Share as much code as possible with the Distributed Runtime.

## Differences from the Distributed Runtime

- There are no job or task schedulers.

## Usage

```clojure
(:require [onyx-local-rt.api :as api])

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 20}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/batch-size 20}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 20}]
   :lifecycles []})

(pprint
 (-> (api/init job)
     (api/new-segment :in {:n 41})
     (api/new-segment :in {:n 84})
     (api/drain)
     (api/stop)
     (api/env-summary)))

;; =>
;; {:next-action :lifecycle/start-task?,
;;  :tasks
;;  {:in {:inbox []},
;;   :inc {:inbox []},
;;   :out {:inbox [], :outputs [{:n 42} {:n 85}]}}}
```

## License

Copyright © 2016 Distributed Masonry

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
