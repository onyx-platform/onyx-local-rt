# onyx-local-rt

onyx-local-rt is an alternative runtime for Onyx that executes jobs in a pure, deterministic environment. This runtime is local-only, and does not run in a distributed mode. This is an incubating repository, meaning this
code will be moved into Onyx core at a later date.

## Goals

- Offer a subset of the Distributed Runtime functionality in an easier-to-configure environment.
- Target ClojureScript as an underlying execution environment.
- Guarantee ordering of message flow throughout jobs.
- Share as much code as possible with the Distributed Runtime.

## Differences from the Distributed Runtime

- There are no job or task schedulers.
- There is no backpressure.
- There is no multi-node execution or network calls.
- There are no threads, parallelism, or atoms.
- There are no peers or virtual peers.
- There are no fault tolerance mechanisms.
- Input and output plugins are ignored, use the API to put segments into/take segments out of the runtime.
- This runtime is not designed to be ultra-high performance.

## Usage

In your `project.clj`:

```
[org.onyxplatform/onyx-local-rt "0.11.1.0"]
```

First, Require the only file, `api.cljc`, and define any functions that will be used.
Next, call `init` with your job. You can then add new segments to an input task (`new-segment`),
move the runtime forward (`tick`), move the runtime forward until all in-flight segments have
reached their outputs (`drain`), or simulate the job shutting down (`stop`). All API functions
take and return the runtime.

View the API on [GitHub Pages](http://www.onyxplatform.org/onyx-local-rt/).

As an example for Clojure(Script):

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
