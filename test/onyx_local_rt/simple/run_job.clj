(ns onyx-local-rt.simple.run-job
  (:require [onyx-local-rt.api :as api]
            [clojure.test :refer [deftest is testing]]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def test-states (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (swap! test-states conj extent-state))

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
               :trigger/id :my-trigger
               :trigger/threshold [1 :elements]
               :trigger/sync ::update-atom!}]
   :lifecycles []})

(deftest run-job-test
  (reset! test-states [])
   (is (= {:next-action :lifecycle/start-task?, 
           :tasks {:inc {:inbox []
                         :window-contents
                         {:collect-segments
                          [[1
                            [{:n 42, :my-key :a}
                             {:n 85, :my-key :a}
                             {:n 5, :my-key :c}]]]}}, 
                  :out {:inbox [], 
                        :outputs [{:n 42 :my-key :a} 
                                  {:n 85 :my-key :a}
                                  {:n 5 :my-key :c}]}, 
                  :in {:inbox []}}}
         (-> (api/init job)
             (api/new-segment :in {:n 41 :my-key :a})
             (api/new-segment :in {:n 84 :my-key :a})
             (api/new-segment :in {:n 4 :my-key :c})
             (api/drain)
             (api/stop)
             (api/env-summary))))
   (is (= [[{:n 42, :my-key :a}]
           [{:n 42, :my-key :a} {:n 85, :my-key :a}]
           [{:n 42, :my-key :a} {:n 85, :my-key :a} {:n 5, :my-key :c}]]
	  @test-states)))
