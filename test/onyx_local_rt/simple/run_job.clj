(ns onyx-local-rt.simple.run-job
  (:require [onyx-local-rt.api :as api]
            [onyx.refinements]
            [onyx.windowing.aggregation]
            [clojure.test :refer [deftest is testing]]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def test-state (atom []))

(defn update-atom! [event 
                    window 
                    trigger 
                    {:keys [task-event lower-bound upper-bound event-type] :as opts} 
                    extent-state]
  (println "Fire test state" @test-state)
  (swap! test-state conj [lower-bound upper-bound extent-state]))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :windows [{:window/id :collect-segments
              :window/task :inc
              :window/type :global
              :window/aggregation [:onyx.windowing.aggregation/min :n]
              :window/init 99}]
   :triggers [{:trigger/window-id :collect-segments
               :trigger/refinement :onyx.refinements/accumulating
               :trigger/on :onyx.triggers/segment
               :trigger/threshold [1 :elements]
               :trigger/fire-all-extents? true
               :trigger/sync ::update-atom!}]
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
             (api/new-segment :in {:n 84})
             (api/new-segment :in {:n 84})
             (api/new-segment :in {:n 84})
             (api/new-segment :in {:n 84})
             (api/new-segment :in {:n 84})
             (api/drain)
             (api/new-segment :in {:n 84})
             (api/drain)
             (api/stop)
             (api/env-summary)))))
