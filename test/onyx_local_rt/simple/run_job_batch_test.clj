(ns onyx-local-rt.simple.run-job-batch-test
  (:require [onyx-local-rt.api :as api]
            [clojure.test :refer [deftest is testing]]))

(def batch-size 5)
(def num-samples 50)
(def batch-tracker (atom []))
(def non-batch-tracker (atom []))

(defn ^:export make-batches [segment]
  (mapv (fn [n] {:n n}) (range (:n segment))))

(defn ^:export run-a-batch [segments]
  (swap! batch-tracker conj segments)
  (mapv (fn [s] {:n2 (:n s)}) segments))

(defn ^:export run-a-non-batch [segment]
  (swap! non-batch-tracker conj segment)
  {:n3 (:n2 segment)})

(def job
  {:workflow [[:in :make-batches]
              [:make-batches :run-a-batch]
              [:run-a-batch :run-a-non-batch]
              [:run-a-non-batch :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 1}
             {:onyx/name :make-batches
              :onyx/type :function
              :onyx/fn ::make-batches
              :onyx/batch-size 1}
             {:onyx/name :run-a-batch
              :onyx/type :function
              :onyx/fn ::run-a-batch
              :onyx/batch-size batch-size
              :onyx/batch-fn? true}
             {:onyx/name :run-a-non-batch
              :onyx/type :function
              :onyx/fn ::run-a-non-batch
              :onyx/batch-size batch-size
              ;; no batch-fn? on this one
              }
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 1}]})

(deftest run-job-batch-test
  (reset! batch-tracker [])
  (reset! non-batch-tracker [])

  (let [env-summary (-> (api/init job)
                        (api/new-segment :in {:n num-samples})
                        (api/drain)
                        (api/stop)
                        (api/env-summary))]

    (testing "workflow executes tasks properly: all inputs are represented in outputs"
      (is (= (mapv (fn [n] {:n3 n}) (range num-samples))
             (get-in env-summary [:tasks :out :outputs]))))

    (testing "batch tasks received batches of segments"
      (is (= (partition batch-size
                        (mapv (fn [n] {:n n}) (range num-samples)))
             @batch-tracker)))

    (testing "non-batch tasks received single segments"
      (is (= (mapv (fn [n] {:n2 n}) (range num-samples))
             @non-batch-tracker)))))
