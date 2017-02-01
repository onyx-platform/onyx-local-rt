(ns onyx-local-rt.simple.run-job
  (:require [onyx-local-rt.api :as api]
            [clojure.test :refer [deftest is testing]]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def test-state (atom nil))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

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
   :windows [{:window/id :collect-segments
              :window/task :inc
              :window/type :global
              :window/aggregation :onyx.windowing.aggregation/conj}]
   :triggers [{:trigger/window-id :collect-segments
               :trigger/refinement :onyx.refinements/accumulating
               :trigger/fire-all-extents? true
               :trigger/on :onyx.triggers/segment
               :trigger/threshold [1 :elements]
               :trigger/sync ::update-atom!}]
   :lifecycles []})

(deftest run-job-test
  (reset! test-state [])
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
             (api/env-summary))))
   (is (= @test-state [{:n 42} {:n 85}])))
