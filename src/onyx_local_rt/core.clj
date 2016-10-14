(ns onyx-local-rt.core
  (:require [com.stuartsierra.dependency :as dep]
            [onyx.static.util :refer [kw->fn]]
            [onyx.static.planning :refer [find-task]]
            [onyx.lifecycles.lifecycle-compile :as lc]))

;;; Functions for example

(defn my-inc [segment]
  (update-in segment [:n] inc))

;;;

(defn restv [xs]
  (or (vec (rest xs)) []))

(defn lifecycles->event-map
  [{:keys [onyx.core/lifecycles onyx.core/task] :as event}]
  (update event
          :onyx.core/compiled
          (fn [compiled]
            (-> compiled
                (assoc :compiled-start-task-fn
                       (lc/compile-start-task-functions lifecycles task))
                (assoc :compiled-before-task-start-fn
                       (lc/compile-before-task-start-functions lifecycles task))
                (assoc :compiled-before-batch-fn
                       (lc/compile-before-batch-task-functions lifecycles task))
                (assoc :compiled-after-read-batch-fn
                       (lc/compile-after-read-batch-task-functions lifecycles task))
                (assoc :compiled-after-batch-fn
                       (lc/compile-after-batch-task-functions lifecycles task))
                (assoc :compiled-after-task-fn
                       (lc/compile-after-task-functions lifecycles task))
                (assoc :compiled-handle-exception-fn
                       (lc/compile-handle-exception-functions lifecycles task))))))

(def action-sequence
  {:lifecycle/start-task? :lifecycle/before-task-start
   :lifecycle/before-task-start :lifecycle/before-batch
   :lifecycle/before-batch :lifecycle/read-batch
   :lifecycle/read-batch :lifecycle/after-read-batch 
   :lifecycle/after-read-batch :lifecycle/apply-fn
   :lifecycle/apply-fn :lifecycle/write-batch
   :lifecycle/write-batch :lifecycle/after-batch
   :lifecycle/after-batch :lifecycle/before-batch
   :lifecycle/after-task-stop :lifecycle/start-task?})

(defmulti apply-action
  (fn [env task action]
    action))

(defmethod apply-action :lifecycle/start-task?
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-start-task-fn])]
    {:task (assoc task :start-task? (f (:event task)))}))

(defmethod apply-action :lifecycle/before-task-start
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-before-task-start-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/before-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-before-batch-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/read-batch
  [env {:keys [inbox] :as task} action]
  {:task
   (-> task
       (assoc :focused-segment (first inbox))
       (assoc :inbox (restv inbox)))})

(defmethod apply-action :lifecycle/after-read-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-read-batch-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/apply-fn
  [env {:keys [focused-segment event] :as task} action]
  {:task
   (if focused-segment
     (update-in task [:focused-segment] (:onyx.core/fn event))
     task)})

(defmethod apply-action :lifecycle/write-batch
  [env {:keys [focused-segment children] :as task} action]
  (cond (and focused-segment (seq children))
        {:task (dissoc task :focused-segment)
         :writes (zipmap children (repeat (vector focused-segment)))}

        focused-segment
        {:task (-> task
                   (dissoc :focused-segment)
                   (update-in [:outputs] conj focused-segment))
         :writes {}}

        :else
        {:task task
         :writes {}}))

(defmethod apply-action :lifecycle/after-batch
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-batch-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defmethod apply-action :lifecycle/after-task-stop
  [env task action]
  (let [f (get-in task [:event :onyx.core/compiled :compiled-after-task-fn])
        event (:event task)]
    {:task (assoc task :event (merge event (f event)))}))

(defn workflow->sierra-graph [workflow]
  (reduce
   (fn [result [src dst]]
     (dep/depend result dst src))
   (dep/graph)
   workflow))

(defn precompile-onyx-fn [catalog-entry]
  (if-let [f (:onyx/fn catalog-entry)]
    (kw->fn f)
    clojure.core/identity))

(defn init-task-state [graph lifecycles task-name catalog-entry]
  (let [children (into #{} (dep/immediate-dependents graph task-name))
        base {:inbox []
              :start-task? false
              :focused-segment nil
              :children children
              :event (lifecycles->event-map
                      {:onyx.core/task task-name
                       :onyx.core/lifecycles lifecycles
                       :onyx.core/task-map catalog-entry
                       :onyx.core/fn (precompile-onyx-fn catalog-entry)})}]
    (if (seq children)
      {task-name base}
      {task-name (assoc base :outputs [])})))

(defn init-task-states [workflow catalog lifecycles graph]
  (let [tasks (reduce into #{} workflow)]
    (apply merge
           (map
            (fn [task-name]
              (let [catalog-entry (find-task catalog task-name)]
                (init-task-state graph lifecycles task-name catalog-entry)))
            tasks))))

(defn init [{:keys [workflow catalog lifecycles] :as job}]
  (let [graph (workflow->sierra-graph workflow)]
    {:tasks (init-task-states workflow catalog lifecycles graph)
     :sorted-tasks (dep/topo-sort graph)
     :pending-writes {}
     :next-action :lifecycle/start-task?}))

(defn integrate-task-updates [env action]
  (reduce
   (fn [result task-name]
     (let [task-state (get-in env [:tasks task-name])
           rets (apply-action env task-state action)
           merge-f (partial merge-with into)]
       (-> result
           (assoc-in [:tasks task-name] (:task rets))
           (update-in [:pending-writes] merge-f (:writes rets)))))
   env
   (:sorted-tasks env)))

(defn transfer-pending-writes [env]
  (let [writes (:pending-writes env)]
    (reduce-kv
     (fn [result task-name segments]
       (update-in result [:tasks task-name :inbox] into segments))
     (assoc env :pending-writes {})
     writes)))

(defn transition-action-sequence [env action]
  (if (and (= action :lifecycle/start-task?)
           (not (every? true? (map :start-task? (vals (:tasks env))))))
    (assoc env :next-action :lifecycle/start-task?)
    (assoc env :next-action (action-sequence action))))

(defn tick [env]
  (let [this-action (:next-action env)]
    (-> env
        (integrate-task-updates this-action)
        (transfer-pending-writes)
        (transition-action-sequence this-action))))

(defn drained? [env]
  (let [task-states (vals (:tasks env))
        inboxes (map :inbox task-states)
        focused-segments (map :focused-segment task-states)]
    (and (every? (comp not seq) inboxes)
         (every? nil? focused-segments))))

(defn drain
  ([env] (drain env 10000))
  ([env max-ticks]
   (loop [env env
          i 0]
     (cond (> i max-ticks)
           (throw (ex-info (format "Ticked %s times and never drained, stopping." max-ticks) {}))

           (drained? env) env

           :else (recur (tick env) (inc i))))))

(defn new-segment [env input-task segment]
  (update-in env [:tasks input-task :inbox] conj segment))

(defn stop [env]
  (let [this-action :lifecycle/after-task-stop]
    (-> env
        (integrate-task-updates this-action)
        (transition-action-sequence this-action))))

(defn env-summary [env]
  {:next-action (:next-action env)
   :tasks
   (reduce
    (fn [result task-name]
      (let [tm (get-in env [:tasks task-name :event :onyx.core/task-map])
            inbox (get-in env [:tasks task-name :inbox])]
        (if (= (:onyx/type tm) :output)
          (let [outputs (get-in env [:tasks task-name :outputs])]
            (assoc result task-name {:inbox inbox :outputs outputs}))
          (assoc result task-name {:inbox inbox}))))
    {}
    (keys (:tasks env)))})

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
