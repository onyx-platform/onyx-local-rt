(ns onyx-local-rt.simple.run-job
  (:require [onyx-local-rt.api :as api]
            [clojure.test :refer [deftest is testing]]))

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

(deftest run-job-test
  (is (= {:next-action :lifecycle/start-task?, 
          :tasks {:inc {:inbox []}, 
                  :out {:inbox [], 
                        :outputs [{:n 42} {:n 85}]}, 
                  :in {:inbox []}}}
         (-> (api/init job)
             (api/new-segment :in {:n 41})
             (api/new-segment :in {:n 84})
             (api/drain)
             (api/stop)
             (api/env-summary)))))
