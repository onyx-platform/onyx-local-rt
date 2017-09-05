(ns onyx-local-rt.simple.run-job-emit-test
  (:require [onyx-local-rt.api :as api]
            [clojure.test :refer [deftest is testing]]))

;; ^:export the function if using in ClojureScript.
(defn ^:export my-inc [segment]
  (update-in segment [:n] inc))

(def test-states (atom []))

(defn update-atom! [event window trigger {:keys [lower-bound upper-bound 
                                                 event-type group-key] :as state-event} 
                    extent-state]
  (swap! test-states conj [group-key lower-bound upper-bound extent-state]))

(defn emit-segment [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  {:event-type event-type
   :window/id (:window/id window)
   :trigger/id (:trigger/id trigger)
   :state extent-state})

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 20}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/group-by-key :my-key
              :onyx/batch-size 20}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 20}]
   :windows [{:window/id :collect-segments
              :window/task :inc
              :window/type :session
              :window/window-key :event-time
              :window/timeout-gap [5 :minutes]
              :window/aggregation :onyx.windowing.aggregation/conj}]
   :triggers [{:trigger/window-id :collect-segments
               :trigger/fire-all-extents? true
               :trigger/on :onyx.triggers/segment
               :trigger/id :my-trigger
               :trigger/emit ::emit-segment
               :trigger/threshold [1 :elements]
               :trigger/sync ::update-atom!}]
   :lifecycles []})

(deftest run-job-test
  (reset! test-states [])
  (is (= {:next-action :lifecycle/start-task?, 
	  :tasks {:inc {:inbox []
                        :window-contents {:collect-segments
                                          {:c
                                           {[1442113680830 1442113680830]
                                            [{:n 5,
                                              :my-key :c,
                                              :event-time
                                              #inst "2015-09-13T03:08:00.830-00:00"}]},
                                           :a
                                           {[1442113200829 1442113380829]
                                            [{:n 42,
                                              :my-key :a,
                                              :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                                             {:n 85,
                                              :my-key :a,
                                              :event-time
                                              #inst "2015-09-13T03:03:00.829-00:00"}]
                                            [1442113680830 1442113680830]
                                            [{:n 86,
                                              :my-key :a,
                                              :event-time
                                              #inst "2015-09-13T03:08:00.830-00:00"}]}}}}, 
		  :out {:inbox [], 
                        :outputs [{:n 42 :my-key :a :event-time #inst "2015-09-13T03:00:00.829-00:00"} 
                                  {:n 85 :my-key :a :event-time #inst "2015-09-13T03:03:00.829-00:00"}
                                  {:n 86 :my-key :a :event-time #inst "2015-09-13T03:08:00.830-00:00"}
                                  {:n 5 :my-key :c :event-time #inst "2015-09-13T03:08:00.830-00:00"}
                                  {:event-type :new-segment,
                                   :window/id :collect-segments,
                                   :trigger/id :my-trigger,
                                   :state
                                   [{:n 5,
                                     :my-key :c,
                                     :event-time #inst "2015-09-13T03:08:00.830-00:00"}]}
                                  {:event-type :new-segment,
                                   :window/id :collect-segments,
                                   :trigger/id :my-trigger,
                                   :state
                                   [{:n 86,
                                     :my-key :a,
                                     :event-time #inst "2015-09-13T03:08:00.830-00:00"}]}
                                  {:event-type :new-segment,
                                   :window/id :collect-segments,
                                   :trigger/id :my-trigger,
                                   :state
                                   [{:n 42,
                                     :my-key :a,
                                     :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                                    {:n 85,
                                     :my-key :a,
                                     :event-time #inst "2015-09-13T03:03:00.829-00:00"}]}
                                  {:event-type :new-segment,
                                   :window/id :collect-segments,
                                   :trigger/id :my-trigger,
                                   :state
                                   [{:n 42,
                                     :my-key :a,
                                     :event-time #inst "2015-09-13T03:00:00.829-00:00"}
                                    {:n 85,
                                     :my-key :a,
                                     :event-time #inst "2015-09-13T03:03:00.829-00:00"}]}
                                  {:event-type :new-segment,
                                   :window/id :collect-segments,
                                   :trigger/id :my-trigger,
                                   :state
                                   [{:n 42,
                                     :my-key :a,
                                     :event-time
                                     #inst "2015-09-13T03:00:00.829-00:00"}]}]}, 
                                  :in {:inbox []}}}
	 (-> (api/init job)
	     (api/new-segment :in {:n 41 :my-key :a :event-time #inst "2015-09-13T03:00:00.829-00:00"})
	     (api/new-segment :in {:n 84 :my-key :a :event-time #inst "2015-09-13T03:03:00.829-00:00"})
	     (api/new-segment :in {:n 85 :my-key :a :event-time #inst "2015-09-13T03:08:00.830-00:00"})
	     (api/new-segment :in {:n 4 :my-key :c :event-time #inst "2015-09-13T03:08:00.830-00:00"})
	     (api/drain)
	     (api/stop)
	     (api/env-summary))))
  (is (= [[:a 1442113200829 1442113200829 [{:n 42, :my-key :a, :event-time #inst  "2015-09-13T03:00:00.829-00:00"}]] 
	  [:a 1442113200829 1442113380829 [{:n 42, :my-key :a, :event-time #inst  "2015-09-13T03:00:00.829-00:00"} 
                                           {:n 85, :my-key :a, :event-time #inst  "2015-09-13T03:03:00.829-00:00"}]]
	  [:a 1442113200829 1442113380829 [{:n 42, :my-key :a, :event-time #inst  "2015-09-13T03:00:00.829-00:00"} 
                                           {:n 85, :my-key :a, :event-time #inst  "2015-09-13T03:03:00.829-00:00"}]]
	  [:a 1442113680830 1442113680830 [{:n 86, :my-key :a, :event-time #inst  "2015-09-13T03:08:00.830-00:00"}]] 
	  [:c 1442113680830 1442113680830 [{:n 5, :my-key :c, :event-time #inst "2015-09-13T03:08:00.830-00:00"}]]]
	 @test-states)))
